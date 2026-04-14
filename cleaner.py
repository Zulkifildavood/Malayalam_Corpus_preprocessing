# correction after feedback stage 1
import os
import glob
import re
import unicodedata
import concurrent.futures
import time
import json
import multiprocessing
from pathlib import Path

# =====================================================================
# MODULARIZED REGEX (Feedback 3.5: Maintainability)
# =====================================================================
# Re-ordered by cost: simple patterns first, complex patterns last
RE_URL = re.compile(r'http|www')
RE_SYMBOLS = re.compile(r'[\[\]\(\)\{\}/\\\\]')
RE_EMOJI_SYMBOLS = re.compile(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\u2600-\u26FF\u2700-\u27BF]')
RE_DATE_TIME = re.compile(r'AM|PM|am|pm|\d{2}[:\.]\d{2}|\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2}')
RE_WEIRD_EDGES = re.compile(r'^\d|\d$|^[a-zA-Z]|^.[a-zA-Z]|[a-zA-Z]$|[a-zA-Z].$')
RE_LONG_NUMS = re.compile(r'\d{6,}')
RE_SEVEN_DIGITS = re.compile(r'(?:\D*\d){7}')
RE_IMPURE_PUNCT = re.compile(r'!{2,}|\?{2,}|\.{2,}')

HEAVY_EXCLUSION_REGEXES = [
    RE_URL, RE_SYMBOLS, RE_EMOJI_SYMBOLS, 
    RE_DATE_TIME, RE_WEIRD_EDGES, RE_LONG_NUMS, RE_SEVEN_DIGITS
]

RE_ENGLISH_CHAR = re.compile(r'[a-zA-Z]')
PURE_PATTERN = re.compile(r'^[0-9\u0D00-\u0D7F\u200C\u200D\u2026\(\),\.\?\'! \s]+$')
#PURE_PATTERN = re.compile(r'^[0-9\u0D00-\u0D7F\u200C\u200D\u2026(),.?\'!\s]+$')

# =====================================================================
# HELPER FUNCTIONS
# =====================================================================
def safe_int_input(prompt, default=None):
    while True:
        value = input(prompt).strip()
        if not value: return default
        if value.isdigit(): return int(value)
        print("❌ Please enter a valid number.")

def is_meaningful(words):
    if not words: return False
    return len("".join(words[:4])) >= 8

def is_pure(text):
    if '"' in text or "\\" in text: return False
    
    # NEW: If text contains excessive punctuation, fail the pure check -> sends to Impure
    if RE_IMPURE_PUNCT.search(text): 
        return False
        
    return bool(PURE_PATTERN.match(text))

def flush_buffer(buffer, file_obj):
    """Writes accumulated records to disk in one batch (Feedback 3.2)."""
    if buffer:
        file_obj.write('\n'.join(buffer) + '\n')
        buffer.clear()

# =====================================================================
# STATELESS WORKER FUNCTION (Feedback 3.1: No IPC Bottlenecks)
# =====================================================================
def process_single_file(file_path: str, pure_dir: str, impure_dir: str, excluded_dir: str, lines_per_file: int):
    path_obj = Path(file_path)
    filename = path_obj.name      # e.g., "data_v1.txt"
    file_prefix = path_obj.stem   # e.g., "data_v1" (Your goal)
    
    # State tracking
    stats = {'pure': 0, 'impure': 0, 'excluded': 0}
    chunks = {'pure': 1, 'impure': 1, 'excluded': 1}
    lines = {'pure': 0, 'impure': 0, 'excluded': 0}
    
    # New Tag Mapping (Requirement: P=Pure, I=Impure, E=Excluded)
    tag_map = {'pure': 'P', 'impure': 'I', 'excluded': 'E'}
    dir_map = {'pure': pure_dir, 'impure': impure_dir, 'excluded': excluded_dir}
    
    # Buffers (Feedback 3.2: 2x-5x faster I/O)
    BATCH_SIZE = 5000 
    buffers = {'pure': [], 'impure': [], 'excluded': []}
    
    # Open initial output files with the new tags
    files = {
        k: open(os.path.join(dir_map[k], f"{file_prefix}_{tag_map[k]}_clean_{chunks[k]:02d}.jsonl"), 'w', encoding='utf-8')
        for k in tag_map
    }
    
    def process_route(stream_type, text_to_write):
        tag = tag_map[stream_type]
        stats[stream_type] += 1
        lines[stream_type] += 1
        
        # S_ID now correctly reflects P, I, or E reference
        json_line = json.dumps({
            "S_ID": f"S_{file_prefix}_{tag}_{stats[stream_type]:07d}", 
            "text": text_to_write
        }, ensure_ascii=False)
        
        buffers[stream_type].append(json_line)
        
        # Batch write logic
        if len(buffers[stream_type]) >= BATCH_SIZE:
            flush_buffer(buffers[stream_type], files[stream_type])
            
        # File rotation logic
        if lines[stream_type] >= lines_per_file:
            flush_buffer(buffers[stream_type], files[stream_type])
            files[stream_type].close()
            chunks[stream_type] += 1
            lines[stream_type] = 0
            files[stream_type] = open(os.path.join(dir_map[stream_type], f"{file_prefix}_{tag}_clean_{chunks[stream_type]:02d}.jsonl"), 'w', encoding='utf-8')

    
    with open(file_path, 'r', encoding='utf-8') as f_in:
        for line in f_in:
            text = line.strip()
            if not text: 
                continue

            # Initialization: Define words early to avoid unbound errors
            words = text.split() 
            is_excluded = False
            
            # --- ROUTE 1: EXCLUDED DATA ---
            # Step 1: Length check (Cheapest)
            if len(text) < 10:
                is_excluded = True
            # Step 2: Word-based checks (Moderate)
            elif len(words) < 4 or not is_meaningful(words):
                is_excluded = True
            elif sum(1 for w in words if RE_ENGLISH_CHAR.search(w)) > 4:
                is_excluded = True
            # Step 3: Heavy Regex (Most expensive)
            elif any(regex.search(text) for regex in HEAVY_EXCLUSION_REGEXES):
                is_excluded = True

            if is_excluded:
                process_route('excluded', text)
                continue

            # --- NORMALIZE & CLEAN ---
            # Now 'words' is guaranteed to exist and is updated
            text = ' '.join(words)
            text = unicodedata.normalize('NFC', text)
            
            # --- ROUTE 2 & 3 ---
            if is_pure(text):
                process_route('pure', text)
            else:
                process_route('impure', text)
            
    # Final Cleanup
    for stream_type in tag_map:
        flush_buffer(buffers[stream_type], files[stream_type])
        files[stream_type].close()
        
        # Delete empty trailing files if a chunk was opened but never used
        if lines[stream_type] == 0:
            tag = tag_map[stream_type]
            empty_file = os.path.join(dir_map[stream_type], f"{file_prefix}_{tag}_clean_{chunks[stream_type]:02d}.jsonl")
            if os.path.exists(empty_file): os.remove(empty_file)
        
    return filename, stats['pure'], stats['impure'], stats['excluded']

# =====================================================================
# MAIN PIPELINE MANAGER
# =====================================================================
def run_multiprocessing_pipeline(input_dir: str, output_dir: str, lines_per_file: int, max_total_pure_files):
    pure_dir = os.path.join(output_dir, "pure")
    impure_dir = os.path.join(output_dir, "impure")
    excluded_dir = os.path.join(output_dir, "excluded")
    
    for d in [pure_dir, impure_dir, excluded_dir]:
        os.makedirs(d, exist_ok=True)
    
    input_files = sorted(glob.glob(os.path.join(input_dir, "*.txt")))
    if not input_files:
        print(f"❌ No .txt files found in '{input_dir}'")
        return

    print(f"\n🚀 Processing {len(input_files)} files utilizing all CPU cores...")
    
    start_time = time.time()
    total_pure = 0
    total_impure = 0
    total_excluded = 0
    
    # Utilizing concurrent workers without shared state bottlenecks (Feedback 3.1)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(process_single_file, filepath, pure_dir, impure_dir, excluded_dir, lines_per_file): filepath 
            for filepath in input_files
        }
        
        for future in concurrent.futures.as_completed(futures):
            filename, p_count, i_count, x_count = future.result()
            print(f"  ✅ {filename} -> Pure: {p_count:,} | Impure: {i_count:,} | Excluded: {x_count:,}")
            
            total_pure += p_count
            total_impure += i_count
            total_excluded += x_count
            
            # Early shutdown if global limit reached
            if total_pure >= (max_total_pure_files * lines_per_file):
                print("🎯 Target reached. Stopping...")
                for f in futures: f.cancel()
                break

    elapsed = time.time() - start_time
    
    # =================================================================
    # METADATA LOG SUMMARY GENERATION
    # =================================================================
    metadata_log = {
        "execution_time_utc": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "pipeline_stage": "Stage 1: P-I-E Cleaning",
        "parameters": {
            "input_dir": input_dir,
            "output_dir": output_dir,
            "lines_per_file_limit": lines_per_file,
            "max_pure_files_target": "ALL" if max_total_pure_files == float('inf') else max_total_pure_files
        },
        "metrics": {
            "elapsed_time_seconds": round(elapsed, 2),
            "total_files_processed": len(input_files),
            "total_pure_records": total_pure,
            "total_impure_records": total_impure,
            "total_excluded_records": total_excluded,
            "total_processed_records": total_pure + total_impure + total_excluded
        }
    }

    log_path = os.path.join(output_dir, "pipeline_execution_log.jsonl")
    with open(log_path, 'a', encoding='utf-8') as log_file:
        log_file.write(json.dumps(metadata_log) + "\n")
    # =================================================================

    print("\n=======================================================")
    print(f"🎉 Stage 1 Complete in {elapsed:.2f} seconds!")
    print(f"🟢 Total PURE (P): {total_pure:,}")
    print(f"🟡 Total IMPURE (I): {total_impure:,}")
    print(f"🗑️  Total EXCLUDED (E): {total_excluded:,}")
    print(f"📝 Metadata log saved to: {log_path}")
    print("\n⚠️  REMINDER: Run Stage 2 for global deduplication.")
    print("=======================================================\n")

if __name__ == "__main__":
    multiprocessing.freeze_support() 
    print("\n⚡️ Malayalam Data Cleaner (P-I-E Standard) ⚡️\n")

    input_path = input("📂 Enter the folder path containing your part_*.txt files:\n> ").strip().strip("'\"")
    output_path = input("\n📁 Enter base output directory (Press Enter to use ./clean_data):\n> ").strip().strip("'\"") or os.path.join(os.getcwd(), "clean_data")

    lines_limit = safe_int_input("\n📦 Enter maximum lines per output chunk (Press Enter for 100,000):\n> ", 100000)
    file_target = safe_int_input(f"\n🎯 Target: Maximum GLOBAL Pure files (Press Enter for ALL):\n> ", float('inf'))

    if os.path.exists(input_path):
        run_multiprocessing_pipeline(input_path, output_path, lines_limit, file_target)
    else:
        print(f"\n❌ Error: Directory '{input_path}' not found.")
