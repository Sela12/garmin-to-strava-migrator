# FIT File to Strava Uploader

This script uploads `.fit` files from any source to your Strava account. It works with FIT files from Garmin, Wahoo, Polar, Suunto, or any other device that exports FIT format. The uploader is designed to be robust, handling rate limits, duplicates, and other potential issues.

## Features

-   **Bulk Upload:** Uploads all `.fit` files from a directory.
-   **Rate Limit Handling:** Automatically handles Strava's rate limits, backing off when required.
-   **Duplicate Detection:** Skips files that have already been uploaded.
-   **Junk File Filtering:** Pre-scans and moves non-activity FIT files (like device logs) to a `_junk` folder to save API quota.
-   **Progress Bars:** Shows progress for both the pre-sweep and the upload process.
-   **After-Action Reports:** Provides a summary of what was done after each step.
-   **Error Handling:** Moves failed uploads to a `_failed` folder for manual inspection.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd strava-fit-uploader
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Get your Strava API credentials:**
    -   Go to [https://www.strava.com/settings/api](https://www.strava.com/settings/api) and create a new application.
    -   You'll get a **Client ID** and a **Client Secret**.

4.  **Authorize the application:**

    The easiest way to get your authorization code is to use the included `oauth_catcher.py` script:

    ```bash
    python archived_scripts/oauth_catcher.py
    ```

    This script will:
    - Open your browser to Strava's authorization page
    - Receive the authorization code when you approve the application
    - Exchange the code for an access token
    - Automatically save both the code and token to your `.env` and `.strava_tokens.json` files

    **Manual Authorization (alternative):**
    
    If you prefer to authorize manually:
    -   Open the following URL in your browser, replacing `YOUR_CLIENT_ID` with your actual Client ID:
        ```
        https://www.strava.com/oauth/authorize?client_id=YOUR_CLIENT_ID&response_type=code&redirect_uri=http://localhost:53682/callback&approval_prompt=auto&scope=activity:read,activity:write
        ```
    -   After authorizing, you'll be redirected to a URL like `http://localhost:53682/callback?code=YOUR_AUTH_CODE`.
    -   Copy the `code` value from the URL. This is your **Authorization Code**.
    -   Paste it into your `.env` file as shown in the next step.

5.  **Configure the environment:**
    -   Create a `.env` file in the project root. You can copy `.env.example` to get started:
        ```bash
        cp .env.example .env
        ```
    -   Edit the `.env` file and fill in your details:
        ```
        CLIENT_ID=your_client_id
        CLIENT_SECRET=your_client_secret
        AUTH_CODE=your_auth_code
        FIT_FOLDER=/path/to/your/fit/files
        MAX_CONCURRENT=5
        ```
        - `MAX_CONCURRENT` controls how many files upload simultaneously (default: 5)

6.  **Getting your FIT files**

    The uploader works with FIT files from any source. Choose the option that matches your device:

    **Option 1: From Any Device/Folder**
    
    Simply point `FIT_FOLDER` in your `.env` file to any folder containing `.fit` files. The uploader will process all FIT files it finds, regardless of the source device.

    **Option 2: Export from Garmin Connect**

    1.  Go to [https://www.garmin.com/en-US/account/datamanagement/](https://www.garmin.com/en-US/account/datamanagement/).
    2.  Select "Export Your Data".
    3.  Request an export of your data. This can take a while (often several hours).
    4.  Once your data is ready, you will receive an email with a download link.
    5.  Download and unzip the file. Inside, you will find a folder named `DI_CONNECT/DI-Connect-Fitness-Uploaded-Files`. This folder contains your `.fit` files.
    6.  Set the `FIT_FOLDER` in your `.env` file to the path of this folder.

    **Option 3: Export from Wahoo ELEMNT**

    1.  Connect your Wahoo device to your computer via USB.
    2.  Navigate to the device storage and locate the `.fit` files (usually in a `workouts` or similar folder).
    3.  Copy the FIT files to a local folder.
    4.  Set the `FIT_FOLDER` in your `.env` file to the path of that folder.

    **Option 4: Export from Polar Flow**

    1.  Log in to [https://flow.polar.com](https://flow.polar.com).
    2.  Go to your training diary and select the activities you want to export.
    3.  Export each activity (Polar allows downloading as FIT or other formats).
    4.  Collect all FIT files into a single folder.
    5.  Set the `FIT_FOLDER` in your `.env` file to the path of that folder.

    **Option 5: Export from Suunto**

    1.  Log in to [https://www.suunto.com](https://www.suunto.com).
    2.  Navigate to your training log and export activities (check if batch export is available).
    3.  Download the FIT files to a local folder.
    4.  Set the `FIT_FOLDER` in your `.env` file to the path of that folder.

    **Note:** Some devices may require third-party tools or manual synchronization to extract FIT files. Check your device's documentation for specific export instructions.

## Usage

Simply run the `main.py` script:

```bash
python main.py
```

The script will:
1.  Perform a pre-sweep to move junk files.
2.  Upload the remaining FIT files to Strava.
3.  Provide summary reports for both steps.

## Technical Architecture

This section explains how the uploader works internally, for developers interested in extending or understanding the codebase.

### Overall Flow

```
┌─────────────────────────────────┐
│  main.py                        │
│  - Load .env config             │
│  - Initialize logging           │
└──────────┬──────────────────────┘
           │
           ├─→ FitCleaner.run() (Pre-sweep)
           │   └─ Inspect FIT files, move junk to _junk/
           │
           └─→ StravaUploader.run() (Upload)
               └─ AsyncStravaUploader.run_async()
                  ├─ Worker pool (max_concurrent tasks)
                  ├─ AsyncRateLimiter (token bucket)
                  └─ UploadPoller (centralized status checking)
```

### Core Components

#### 1. **Authentication & Token Management** (`auth.py`, `token_store.py`)

- **TokenStore abstraction**: Decouples token persistence from auth logic
  - `FileTokenStore`: Persists tokens to `.strava_tokens.json`
  - `InMemoryTokenStore`: Keeps tokens in memory (testing)
  
- **StravaAuth class**: Manages OAuth flow
  - `exchange_code()`: Exchanges auth code for access token
  - `refresh()`: Renews expired tokens using refresh token
  - `ensure_token()`: Always returns a valid token (refreshing if needed)

#### 2. **Rate Limiting** (`limiter.py`)

The `AsyncRateLimiter` uses a **token bucket** pattern with smart backoff:

- **Cooperative short sleeps**: Instead of blocking a worker for 900 seconds, it sleeps in 5-second increments, allowing task cancellation
- **Jittered exponential backoff**: When rate-limited (429 response), backoff time grows exponentially with random jitter to prevent thundering herd
- **Retry-After header parsing**: Respects server hints when to retry
- **Per-window limits**: Tracks requests within 15-minute windows and daily limits

#### 3. **Async Upload Pipeline** (`async_core.py`)

The uploader runs as an async task pipeline:

1. **Pre-upload preparation**:
   - Scans `FIT_FOLDER` for `.fit` files (case-insensitive)
   - Filters out files in special folders (`_junk`, `_failed`, `_processing`)
   - Enqueues files to a shared `asyncio.Queue`

2. **Worker pool**:
   - Creates `MAX_CONCURRENT` worker tasks (default 5)
   - Each worker pulls from the queue and calls `_upload_single()`
   - If a 429 rate limit is hit, re-queues the file for retry

3. **Per-file upload**:
   - Acquires a permit from the rate limiter
   - Reads the FIT file into memory (closes handle immediately)
   - Creates a multipart form with `data_type=fit` and file content
   - POSTs to Strava's upload endpoint

4. **Centralized status polling** (`poller.py`):
   - Single background task polls upload status
   - Avoids creating hundreds of concurrent status requests
   - Respects rate limits and Retry-After headers
   - Processes results and moves files to appropriate folders

#### 4. **File Organization**

After processing, files are moved into one of four folders:

- **`_uploading/`**: Temporarily holds files during upload (atomic operations)
- **`_junk/`**: Non-activity FIT files (device logs, firmware updates) filtered by pre-sweep
- **`_failed/`**: Files that failed to upload; retain for manual inspection
- **Root folder**: Successfully uploaded files are deleted (no local copy retained)

#### 5. **Logging & Reporting**

- **Log file**: `strava_upload.log` (truncated at startup, appends per-session)
- **After-action report**: Two files timestamped and written to repo root:
  - `after_action_report_YYYYMMDDTHHMMSSZ.json`: Full summary with stats and per-file details
  - `after_action_report_YYYYMMDDTHHMMSSZ.csv`: Spreadsheet-friendly format with file, status, upload_id, activity_id, reason

### Why This Architecture?

**Async/Concurrency**: Strava API requests are I/O-bound. Using `asyncio` allows many uploads to happen concurrently without threads.

**Centralized Polling**: Checking 100 upload statuses concurrently would trigger rate limits. A single background poller respects limits while checking status for all uploads.

**Rate Limiter Design**: Cooperative short sleeps allow task cancellation (useful for Ctrl+C) and prevent blocking workers when hit with 429.

**Token Persistence**: Tokens are refreshed automatically; once authorized, the script runs unattended. Tokens are persisted to `.strava_tokens.json` so re-running doesn't require re-authorization.

**Pre-sweep Cleaning**: Device exports often contain non-activity FIT files (device logs, firmware updates, monitoring data). The pre-sweep filters these out, saving API quota before attempting upload.
