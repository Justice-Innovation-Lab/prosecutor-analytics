# Batch Processing with an AI API

**Date Last Updated:** March 26, 2026

---

## Overview

Batch processing lets you send large volumes of API requests asynchronously instead of one at a time. Most major AI providers (OpenAI, Anthropic, Google, etc.) offer a batch endpoint that accepts a file of requests and returns results within a processing window (typically up to 24 hours).

The general workflow is:

1. **Build** a JSONL file of requests, each tagged with a unique `custom_id`
2. **Submit** the file to the provider's batch endpoint
3. **Poll** for completion
4. **Download** and parse the results
5. **Detect errors** and re-batch any failures

---

## Why Batch?

| Consideration | Synchronous API | Batch API |
|---------------|----------------|-----------|
| Cost | Standard pricing | Typically 50% discount |
| Throughput | Rate-limited | Higher aggregate throughput |
| Latency | Seconds per request | Hours per batch |
| Best for | Interactive / real-time use | Bulk evaluation, benchmarking, scheduled jobs |

Use batch when you can tolerate latency in exchange for cost savings and throughput — e.g., scoring a dataset, running evaluations, or background content generation.

---

## Step 1: Build the Request File (JSONL)

Each line of your JSONL file is a self-contained JSON request object. The exact schema depends on your provider, but the pattern is consistent: an identifier, an endpoint, and a body.

### General Structure

```json
{
  "custom_id": "request-001",
  "method": "POST",
  "url": "/v1/chat/completions",
  "body": {
    "model": "your-model-id",
    "temperature": 0.7,
    "messages": [
      { "role": "system", "content": "Your system prompt." },
      { "role": "user", "content": "Your user prompt with any injected variables." }
    ]
  }
}
```

### Key Principles

- **One JSON object per line** — no pretty-printing, no trailing commas.
- **`custom_id` is critical** — this is how you link responses back to your source data. Encode any metadata you need for reassembly (e.g., `row-42_template-A`).
- **Validate before submitting** — read each line back with a JSON parser to catch syntax errors early.

### Generating the File Programmatically

```python
import json

requests = []
for item in your_dataset:
    request = {
        "custom_id": f"item-{item['id']}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "your-model-id",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": format_prompt(item)}
            ]
        }
    }
    requests.append(request)

with open("batch_requests.jsonl", "w") as f:
    for req in requests:
        f.write(json.dumps(req) + "\n")
```

### Pre-Submit Validation

```python
import json

with open("batch_requests.jsonl") as f:
    for i, line in enumerate(f, 1):
        try:
            obj = json.loads(line)
            assert "custom_id" in obj, f"Line {i}: missing custom_id"
        except (json.JSONDecodeError, AssertionError) as e:
            print(f"Validation error on line {i}: {e}")
```

---

## Step 2: Submit the Batch

Upload your JSONL file and create a batch job. The provider returns a batch/job ID you will use for polling.

### Example (Python SDK Pattern)

```python
# Upload the file
with open("batch_requests.jsonl", "rb") as f:
    uploaded_file = client.files.create(file=f, purpose="batch")

# Create the batch job
batch_job = client.batches.create(
    input_file_id=uploaded_file.id,
    endpoint="/v1/chat/completions",
    completion_window="24h"
)

print(f"Batch ID: {batch_job.id}")
print(f"Status:   {batch_job.status}")
```

Save the batch ID — you will need it for every subsequent step.

---

## Step 3: Poll for Completion

Check the batch status periodically until it reaches a terminal state.

### Common Status Values

| Status | Meaning |
|--------|---------|
| `validating` | Provider is checking the input file format |
| `queued` | Waiting for capacity |
| `in_progress` | Requests are being processed |
| `completed` | All results are ready to download |
| `failed` | The batch failed — check the error file |
| `expired` | Processing exceeded the time window |

### Polling Script

```python
import time

while True:
    batch_job = client.batches.retrieve(batch_job.id)
    status = batch_job.status
    print(f"Status: {status}")

    if status in ("completed", "failed", "expired"):
        break

    time.sleep(60)  # check every minute
```

---

## Step 4: Download and Parse Results

When the status is `completed`, download the results file. It will be a JSONL file with one result per line, keyed by `custom_id`.

### Download

```python
result_content = client.files.content(batch_job.output_file_id).content

with open("batch_results.jsonl", "wb") as f:
    f.write(result_content)
```

### Parse

Each result line contains the original `custom_id`, a response object (or error), and metadata. The exact nesting varies by provider; here is a typical structure:

```json
{
  "id": "response-abc123",
  "custom_id": "request-001",
  "response": {
    "status_code": 200,
    "body": {
      "choices": [
        {
          "message": {
            "role": "assistant",
            "content": "{ ... model output ... }"
          }
        }
      ]
    }
  },
  "error": null
}
```

### Parsing Script

```python
import json

results = []

with open("batch_results.jsonl") as f:
    for line in f:
        result = json.loads(line)
        custom_id = result["custom_id"]
        error = result.get("error")

        if error:
            results.append({"custom_id": custom_id, "error": str(error), "output": None})
            continue

        # Navigate to the model's response text
        content = result["response"]["body"]["choices"][0]["message"]["content"]

        # If you requested JSON output, parse it
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as e:
            results.append({"custom_id": custom_id, "error": f"Invalid JSON: {e}", "output": None})
            continue

        results.append({"custom_id": custom_id, "error": None, "output": parsed})
```

### Linking Back to Source Data

Use `custom_id` to join results with your original dataset:

```python
import pandas as pd

source_df = pd.read_csv("source_data.csv")
results_df = pd.DataFrame(results)

merged = source_df.merge(results_df, on="custom_id", how="left")
merged.to_csv("final_output.csv", index=False)
```

---

## Step 5: Error Handling and Re-Batching

Errors fall into two categories:

### Error Types

| Category | Description | Examples |
|----------|-------------|---------|
| **API-level errors** | The provider returned an error for the request | Rate limit exceeded, invalid model, malformed prompt |
| **Response-level errors** | The request succeeded but the output is unusable | Invalid JSON, missing expected fields, truncated output |

### Detection

```python
failed_rows = [r for r in results if r["error"] is not None]

print(f"Total:    {len(results)}")
print(f"Success:  {len(results) - len(failed_rows)}")
print(f"Errors:   {len(failed_rows)}")
```

For response-level errors, validate the parsed output against your expected schema:

```python
REQUIRED_FIELDS = ["score", "summary", "category"]

for r in results:
    if r["error"] is None and r["output"] is not None:
        missing = [f for f in REQUIRED_FIELDS if f not in r["output"]]
        if missing:
            r["error"] = f"Missing fields: {missing}"
```

### Re-Batch Failed Requests

Extract the `custom_id` values of failed rows, find the corresponding original requests, and write a new JSONL file:

```python
failed_ids = {r["custom_id"] for r in results if r["error"] is not None}

with open("batch_requests.jsonl") as f:
    original_requests = [json.loads(line) for line in f]

retry_requests = [req for req in original_requests if req["custom_id"] in failed_ids]

with open("batch_retry.jsonl", "w") as f:
    for req in retry_requests:
        f.write(json.dumps(req) + "\n")

print(f"Retry batch: {len(retry_requests)} requests")
```

Then repeat Steps 2–4 with `batch_retry.jsonl`. After parsing the retry results, merge them back:

```python
original = pd.read_csv("final_output.csv")
retry = pd.read_csv("retry_output.csv")

# Drop the failed rows from original, replace with retry results
retry_ids = set(retry["custom_id"])
cleaned = original[~original["custom_id"].isin(retry_ids)]
combined = pd.concat([cleaned, retry], ignore_index=True).sort_values("custom_id")

remaining_errors = combined[combined["error"].notna()]
print(f"Combined: {len(combined)} rows, {len(remaining_errors)} remaining errors")

combined.to_csv("final_output_combined.csv", index=False)
```

If errors persist after a retry, consider adjusting your prompt, switching models, or flagging those rows for manual review.

---

## Security Considerations

- **Never hard-code API keys.** Use environment variables or a secrets manager.
- **Sanitize inputs.** If prompts include user-generated data, validate and sanitize before embedding in the request body to prevent prompt injection.
- **Protect output files.** Results may contain sensitive data — restrict file permissions and avoid committing them to version control.
- **Audit `custom_id` values.** Do not embed PII or sensitive identifiers in `custom_id` if batch files are stored in shared or logged environments.
- **Use encryption at rest** for any stored request/result files containing sensitive content.

---

## Suggested Tests

Before running a full batch, validate each stage independently:

1. **JSONL generation** — generate a small file (5–10 requests), verify every line parses as valid JSON, and confirm each `custom_id` is unique.
2. **Submit and poll** — submit the small file, verify the status transitions (validating → queued → in_progress → completed), and confirm the batch ID is retrievable.
3. **Parse round-trip** — download results from the small batch and verify that every `custom_id` appears in the output, the response structure matches your parser, and JSON output deserializes correctly.
4. **Error detection** — intentionally include a malformed request (e.g., invalid model name) and confirm your parser catches it and routes it to the error path.
5. **Re-batch** — confirm that the retry JSONL contains only the failed `custom_id` values and that the merge logic correctly replaces failed rows without duplicating successful ones.
6. **Idempotency** — run the full pipeline twice on the same input and verify the final output is identical (assuming deterministic temperature settings).

---

## Quick-Reference Checklist

```
[ ] Generate JSONL with unique custom_id per request
[ ] Validate JSONL (parseable, no duplicates, required fields present)
[ ] Submit batch and record the batch/job ID
[ ] Poll until terminal status
[ ] Download results file
[ ] Parse responses, capturing both API errors and malformed outputs
[ ] Link results back to source data via custom_id
[ ] Re-batch any failures
[ ] Merge retry results into final dataset
[ ] Verify zero (or acceptable) remaining errors
```
