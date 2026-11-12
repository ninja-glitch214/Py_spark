You can install **Ollama** using a single command in your Windows PowerShell or Linux terminal, then use `ollama pull` to download models and `ollama run` to chat with them.

1. Install Ollama

**Windows**

Open **PowerShell** (from the Start menu) and run either of these commands:

- **Official PowerShell Script** (via [Ollama for Windows](https://www.google.com/goto?url=CAESXwHEIkXXvg2Tz-jSlOnsU6HOPCwKONW-xmwcOzdgKOjuenLKZL41G9dcZeEYhkFL0HL_dX0Yf_1RleDd2sbCX1ATmX9sqmNXKU2brmWW3H-DXk8Lp-bdiQtKoSIPZ7p8)):
    
    powershell
    
    ```
    irm https://ollama.com/install.ps1 | iex
    ```
    
    Use code with caution.
    
- **Using Winget** (via [winstall](https://www.google.com/goto?url=CAESYwHEIkXXLExmj9_tuwPKVTH2gOCyMZLrfC3CCcxoenf5lLaqkJQUPSkTO6zouK2z335de_B6ofG8NY4qDyvKBbIwkPY2lqn1E22AGOBxAFxqK4lUSBJx95kJqfqy08Ut48w_og)):
    
    powershell
    
    ```
    winget install -e --id Ollama.Ollama
    ```
    
    Use code with caution.
    

_(Note: After installation finishes, open a **new** PowerShell or Command Prompt window so the `ollama` command is recognized)._

**Linux**

Open your terminal and run the official install script from [Ollama for Linux](https://www.google.com/goto?url=CAESXQHEIkXXi5E4vwtoz3uMTfoaEacskPVC0ZML4hz2EBdDJdMv1tUCk1tV-RaZzP-DJgLwstcO98AaLj1vsI9FGX1boJg2eEmk1JJWTTXhePJTwrjageaAatTaxz0pxA) (requires `curl` and `sudo` privileges):

bash

```
curl -fsSL https://ollama.com/install.sh | sh
```

Use code with caution.

_(If `curl` is missing on Ubuntu/Debian, run `sudo apt update && sudo apt install -y curl` first)._

---

2. Download & Run the Models

Once Ollama is installed, the commands are identical on both **Windows** and **Linux**.

**Option A: Download AND Run Immediately (Recommended)**

`ollama run` automatically downloads the model if you don't have it yet, then opens an interactive chat prompt in your terminal:

- **Qwen2.5-Coder 7B** (Best balance for PySpark/Hive/HBase on 8GB RAM):
    
    bash
    
    ```
    ollama run qwen2.5-coder:7b
    ```
    
    Use code with caution.
    
- **Qwen2.5-Coder 14B** (Higher accuracy for complex Spark/Hive queries, needs 16GB+ RAM/VRAM):
    
    bash
    
    ```
    ollama run qwen2.5-coder:14b
    ```
    
    Use code with caution.
    
- **DeepSeek-R1 7B** (Step-by-step reasoning for debugging errors):
    
    bash
    
    ```
    ollama run deepseek-r1:7b
    ```
    
    Use code with caution.
    
- **Phi-3.5 Mini** (Lightweight option for 4–6GB RAM):
    
    bash
    
    ```
    ollama run phi3.5
    ```
    
    Use code with caution.
    

**Option B: Download Only (Without Starting Chat)**

If you want to download a model in the background to use later (for example, inside VS Code), use `ollama pull`:

bash

```
ollama pull qwen2.5-coder:7b
```

Use code with caution.

---

3. Useful Management Commands

- **Exit the model chat:** Type `/bye` and press **Enter**.
- **See all downloaded models:**
    
    bash
    
    ```
    ollama list
    ```
    
    Use code with caution.
    
- **Delete a model to free up disk space:**
    
    bash
    
    ```
    ollama rm qwen2.5-coder:7b
    ```
    
    Use code with caution.
    
- **Check if Ollama is running:**
    
    bash
    
    ```
    ollama --version
    ```
    
    Use code with caution.
    

Would you like instructions on how to connect these local models to **VS Code** (for inline PySpark/SQL autocomplete and chat) or a **browser-based ChatGPT-style UI**?
