As a Senior Data Engineer, I have reviewed the structure and technical approach of your pipeline. Your strategy of utilizing kernel-level utilities like `split` on newline boundaries to avoid multi-byte UTF-8 truncation is an excellent, industry-standard practice for handling Malayalam's complex script. Furthermore, implementing the P-I-E classification alongside a multiprocessing architecture demonstrates a robust understanding of large-scale NLP data preparation.

Here is the rewritten, heavily optimized README. It retains every technical detail and pedagogical insight from your original draft but organizes the information into a highly professional, scannable, and authoritative documentation format suitable for a top-tier GitHub repository.

***

# Malayalam NLP Corpus Cleaner (P-I-E Standard)

A high-performance Python pipeline engineered to extract, clean, and categorize large-scale Malayalam text corpora (e.g., CC100) for NLP research. Designed to tackle the specific morphological and agglutinative complexities of Dravidian languages, this tool transitions raw, noisy `.txt` datasets (8GB+) into structured, LLM-ready `.jsonl` files.

## 🚀 Overview & P-I-E Classification

Processing Malayalam data introduces unique linguistic and encoding challenges. This pipeline introduces the **P-I-E Classification Standard** to ensure dataset quality and traceability for pre-training or fine-tuning workflows:

* **🟢 Pure (P):** High-quality, grammatically structured Malayalam text devoid of noise.
* **🟡 Impure (I):** Linguistically valuable text containing minor artifacts, non-standard punctuation, or mixed-script noise.
* **🗑️ Excluded (E):** Non-sentential data, heavily non-Malayalam content, code snippets, or URLs.

## ✨ Core Architecture

* **Parallel Processing Engine:** Maximizes hardware utilization by parallelizing batch operations across all available CPU cores using `concurrent.futures`.
* **Optimized Batch I/O:** Drastically reduces disk write overhead through memory buffering (processing and writing in batches of 5,000 lines).
* **Asymptotic Regex Filtering:** Implements a tiered validation system. Low-compute heuristics (string length, word density) are evaluated before executing high-compute regular expressions.
* **Strict Unicode Normalization:** Enforces `NFC` (Normalization Form Canonical Composition) to guarantee uniform character representation, preventing vector fragmentation during tokenization.
* **Audit & Metadata Logging:** Automatically generates `pipeline_log.jsonl` to track execution metrics, throughput, and state parameters.

---

## 📁 Repository Structure

```text
├── clean_data/             # Automated output directory
│   ├── pure/               # Cleaned JSONL files (Ready for LLM training)
│   ├── impure/             # Secondary quality data (Requires manual validation)
│   ├── excluded/           # Discarded noise
│   └── pipeline_log.jsonl  # Execution metadata and pipeline statistics
├── cleaner.py              # Main multi-threaded processing script
└── README.md
```

---

## 🛠️ Setup & Execution Workflow

### 1. Prerequisites & Installation
* **Environment:** Python 3.8+
* **Dependencies:** Standard library only (`re`, `multiprocessing`, `unicodedata`). No external packages required.

```bash
git clone https://github.com/your-username/malayalam-nlp-cleaner.git
cd malayalam-nlp-cleaner
```

### 2. Data Acquisition
This pipeline is optimized for the **CC-100 Malayalam** dataset.
1.  Download `ml.txt.xz` from the [CC-100 Repository](https://autonlp.ai/datasets/cc100-malayalam).
2.  Decompress the archive to extract the raw `ml.txt` file.

### 3. High-Performance Pre-processing (Data Chunking)
Because an 8GB text file exceeds optimal memory boundaries, it must be sequentially sharded before Python processing. We utilize the Unix `split` command for O(n) kernel-level segmentation.

```bash
# Monitor throughput with Pipe Viewer (pv) and split into 100k-line chunks
pv file_name.txt | split -l 100000 - mal_ --additional-suffix=.txt
```

#### The Engineering Logic Behind Safe Chunking
Malayalam characters are represented via **Multi-byte UTF-8 encoding**. Splitting a file strictly by byte size (`-b`) risks severing a character mid-byte, resulting in fatal `UnicodeDecodeError` exceptions in Python.
By passing the `-l` (lines) flag, the buffer is split exactly at the newline character (`\n` / `0x0A`), an ASCII single byte. This guarantees **100% data and Aksharam integrity** across all output chunks (`mal_aa.txt`, `mal_ab.txt`, etc.).

| Flag / Argument | Technical Impact |
| :--- | :--- |
| `split` | Initiates a sequential, zero-buffer read-stream from the disk. |
| `-l 100000` | Redirects the stream to a new file exactly every 100,000 lines. |
| `rawdata_filename.txt` | Target input stream (8GB corpus). |
| `mal_` | The designated prefix for resulting shards. |
| `--additional-suffix=.txt`| Enforces the `.txt` extension for automated ingestion by the Python script. |

### 4. Running the Pipeline
Once the data is chunked, execute the primary script to initialize the `ThreadPoolExecutor`.

```bash
python cleaner.py
```
*The CLI will prompt you to define the input directory (containing your chunks), the target output directory, and the maximum lines per chunk.*

---

## 🔍 Stage 1: The Cleaning Logic

The processing script forces raw data through four rigid refinement layers:

1.  **Length & Density Filters:** Automatically prunes sentences under 10 characters or fewer than 4 distinct words.
2.  **Noise Pruning:** Strips URLs, HTML artifacts, timestamps, emojis, and disproportionate Latin/English character usage.
3.  **Linguistic Pattern Matching:**
    * **Pure Data:** Strictly validates against the Malayalam Unicode block (`\u0D00-\u0D7F`), standard numerals, and baseline punctuation.
    * **Impure Data:** Captures valid Malayalam strings corrupted by repetitive symbols (e.g., `!!!`, `...`) or non-standard glyphs.
4.  **JSONL Serialization:** Packages each processed string into a structured JSON object, generating a unique `S_ID` for downstream traceability.

**Sample Output (`pure/chunk_aa.jsonl`):**
```json
{
  "S_ID": "S_part_01_P_0000123",
  "text": "മലയാളം ഒരു ദ്രാവിഡ ഭാഷയാണ്."
}
```

---

## 📊 Pipeline Metrics (Latest Run)

* **Dataset:** 8GB+ CC100 Malayalam
* **Infrastructure:** Local multi-core execution
* **Storage Strategy:** Automated file rotation ensures optimal loading sizes for Hugging Face `datasets` or PyTorch DataLoaders.

> **Status:** Completed
> **Throughput:** ~18,462 records/second

| Metric | Recorded Data |
| :--- | :--- |
| **Total Processed Records** | 22,920,135 |
| **🟢 Total Pure Records** | 6,807,356 |
| **🟡 Total Impure Records** | 6,620,795 |
| **🗑️ Total Excluded Records** | 9,491,984 |
| **Total Files Processed** | 25 |
| **Elapsed Time** | 1,241.46 seconds |

*Configuration used: `100,000` Lines/File Limit | Target: `ALL`*

---

## 🏗️ Future Work

* [ ] **Stage 2:** Global De-duplication across all distributed shards.
* [ ] **Stage 3:** Advanced morphological analysis and sub-word tokenization modeling.

## 🎓 Acknowledgments & Disclaimer

This project was developed as part of a Master's degree research initiative aimed at advancing Malayalam representation in foundational NLP models. Special thanks to the open-source community for providing large-scale raw data like CC100.

**Disclaimer:** *This script is designed for research applications. While the 'Pure' extraction ensures high fidelity, developers should manually inspect samples from the 'Impure' bucket prior to injecting them into production LLM training pipelines.*
