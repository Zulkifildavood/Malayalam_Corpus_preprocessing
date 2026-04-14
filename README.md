This is a comprehensive README file tailored for your GitHub repository. It highlights the technical complexity of the Malayalam language and the efficiency of your multiprocessing pipeline.

***

# Malayalam NLP Corpus Cleaner (P-I-E Standard)

A high-performance Python pipeline designed to extract, clean, and categorize large-scale Malayalam text corpora (e.g., CC100) for NLP research. This tool was developed as part of a Master's research project to address the unique challenges of Malayalam—an agglutinative and morphologically rich Dravidian language.

## 🚀 Overview

Processing Malayalam data is notoriously difficult due to its complex script representation and morphological structure. This script automates the transition from **8GB+ of raw, noisy `.txt` data** to **structured, categorized `.jsonl` files**, ready for Language Model (LLM) pre-training or fine-tuning.

The pipeline introduces the **P-I-E Classification Standard**:
* **🟢 Pure (P):** High-quality, grammatically "clean" Malayalam text.
* **🟡 Impure (I):** Linguistically valuable text containing minor noise or non-standard punctuation.
* **🗑️ Excluded (E):** Non-sentential data, URLs, code snippets, or heavily non-Malayalam content.

## ✨ Key Features

-   **Multiprocessing Engine:** Utilizes all available CPU cores via `concurrent.futures` for maximum throughput.
-   **Optimized Batch I/O:** Implements memory buffering (5,000 lines/batch) to minimize disk write overhead.
-   **Modular Regex Logic:** A tiered filtering system that runs "low-cost" checks (length, word count) before "high-cost" regex patterns.
-   **Unicode Normalization:** Enforces `NFC` normalization to ensure consistent character representation across the dataset.
-   **Structured Metadata:** Generates a detailed execution log including processing time, record counts, and pipeline parameters.

## 📁 Repository Structure

```text
├── clean_data/             # Default output directory
│   ├── pure/               # Cleaned JSONL files (Ready for training)
│   ├── impure/             # Secondary quality data
│   ├── excluded/           # Discarded noise
│   └── pipeline_log.jsonl  # Metadata and stats
├── cleaner.py              # Main processing script
└── README.md
```

## 🛠️ Installation & Usage

### Prerequisites
- Python 3.8+
- No external libraries required (uses standard library: `re`, `multiprocessing`, `unicodedata`)

### Running the Pipeline
1.  Clone the repository:
    ```bash
    git clone https://github.com/your-username/malayalam-nlp-cleaner.git
    cd malayalam-nlp-cleaner
    ```
2.  Run the script:
    ```bash
    python cleaner.py
    ```
3.  Follow the interactive prompts to:
    * Provide the input folder path (containing `.txt` parts).
    * Set the output directory.
    * Define the maximum lines per chunk (default: 100,000).

## 🔍 The Cleaning Logic (Stage 1)

The script processes data through several layers of refinement:

1.  **Length & Density Filter:** Removes strings under 10 characters or fewer than 4 words.
2.  **Noise Identification:** Detects URLs, timestamps, emojis, and excessive English characters.
3.  **Linguistic Pattern Matching:**
    * **Pure:** Only Malayalam characters (`\u0D00-\u0D7F`), digits, and basic punctuation.
    * **Impure:** Text containing valid Malayalam but interrupted by repetitive symbols (`!!!`, `...`) or non-standard characters.
4.  **JSONL Conversion:** Wraps text into a JSON object with a unique `S_ID` for traceability.

### Output Example
```json
{
  "S_ID": "S_part_01_P_0000123",
  "text": "മലയാളം ഒരു ദ്രാവിഡ ഭാഷയാണ്."
}
```

## 📊 Performance Metrics

* **Dataset Processed:** 8GB+ Raw Text (Sourced from [CC100 Malayalam](https://autonlp.ai/datasets/cc100-malayalam)).
* **Speed:** Optimized for local machines using parallel execution.
* **Organization:** Automatic file rotation prevents massive file sizes, making it easier to load data into Hugging Face `datasets` or PyTorch DataLoaders.

## 🏗️ Future Work
- [ ] **Stage 2:** Global De-duplication (Across all shards).
- [ ] **Stage 3:** Advanced Morphological analysis for sub-word tokenization.

## 🎓 Acknowledgments
This project is part of a Master's degree research focused on improving Malayalam representation in NLP. Special thanks to the open-source community for providing raw datasets like CC100.

---
**Disclaimer:** *This script is for research purposes. While it ensures high-quality data extraction, users should manually verify samples from the 'Impure' bucket before use in production models.*
