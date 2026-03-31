# Batch Processing Prompt Template

## How to Use This

This prompt is designed to be pasted into an AI assistant (Claude, ChatGPT, etc.) along with the accompanying **BATCH_PROCESSING.md** workflow guide. Fill in the bracketed sections, attach the workflow doc, and the AI will generate a custom batch processing pipeline tailored to your situation.

---

## The Prompt

```
I need you to help me build a complete batch processing pipeline for sending
requests to an AI API. I've attached a generic batch processing workflow
document — please use it as the structural foundation and customize every
step for my specific situation.

Here is my situation:

## My Data
- **Source data file(s):** [e.g., "a CSV called customer_reviews.csv with columns: id, review_text, category, date"]
- **Number of records:** [e.g., "~2,000 rows"]
- **Unique identifier column:** [e.g., "id" — or describe how to construct one]

## What I Want the AI to Do
- **Task description:** [e.g., "Classify each review as positive, negative, or neutral and extract the top 3 keywords"]
- **Expected output fields:** [e.g., "sentiment (string), confidence (float 0-1), keywords (list of strings)"]
- **Output format:** [e.g., "JSON object per response"]

## My Prompt(s)
- **System prompt:** [Paste your system prompt, or say "please help me write one"]
- **User prompt template:** [Paste your user prompt with placeholders like {review_text}, or say "please help me write one"]
- **Number of prompt variants:** [e.g., "just one" or "three — one per persona I'm testing"]

## API Details
- **Provider:** [e.g., "OpenAI", "Anthropic", "Google Vertex AI"]
- **Model:** [e.g., "gpt-4o-mini", "claude-sonnet-4-20250514"]
- **Temperature:** [e.g., "0.3" or "not sure — please recommend"]
- **Any provider-specific settings:** [e.g., "response_format: json_object" or "none that I know of"]

## My Environment
- **Language:** [e.g., "Python 3.11"]
- **Key libraries already installed:** [e.g., "pandas, openai SDK" or "starting from scratch"]
- **How I manage secrets:** [e.g., "environment variables", ".env file with python-dotenv", "not sure"]

## What I Need You to Produce
Please generate the following, customized to my situation above:

1. **JSONL generation script** — reads my source data, fills in my prompt
   template(s), and writes a validated batch_requests.jsonl file. Include
   a custom_id scheme that lets me link results back to my source rows
   (and prompt variant, if applicable).

2. **Submission script** — uploads the JSONL file, creates the batch job,
   and prints the batch ID.

3. **Polling script** — checks status in a loop and downloads results
   when complete.

4. **Parsing script** — reads the results JSONL, extracts my expected
   output fields, flags errors (both API errors and malformed/incomplete
   responses), and merges everything back to my source data as a CSV.

5. **Re-batch script** — takes the parsed output, filters to error rows,
   rebuilds a retry JSONL from the original requests, and includes a
   merge step to combine retry results with the original successes.

6. **Validation and tests** — for each script, include at minimum:
   - A small-scale dry-run command (e.g., --limit 5)
   - Assertions or checks that confirm the output is correct before
     moving to the next step

7. **Security notes** — flag anything in my setup that needs attention
   (exposed keys, PII in custom_ids, file permissions, etc.)

If any of my inputs above are incomplete or unclear, ask me before
generating code.
```

---

## Tips for Best Results

- **Attach BATCH_PROCESSING.md** to the same conversation so the AI has the full workflow reference.
- **Be specific about your data.** Pasting the first 3–5 rows of your CSV (with sensitive fields redacted) helps the AI write accurate column references.
- **If you have multiple prompt variants,** list each one or describe the pattern (e.g., "I have a `prompts/` folder with three `.txt` files").
- **If you don't know an answer,** say so — the AI can recommend defaults for temperature, model, custom_id format, etc.
- **Iterate.** After the first pass, ask the AI to add logging, progress bars, config files, or whatever else your workflow needs.
