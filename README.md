# Strava FIT File Uploader

This script uploads `.fit` files from a specified folder to your Strava account. It's designed to be robust, handling rate limits, duplicates, and other potential issues.

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
    -   You need to get an authorization code. Open the following URL in your browser, replacing `YOUR_CLIENT_ID` with your actual Client ID:
        ```
        https://www.strava.com/oauth/authorize?client_id=YOUR_CLIENT_ID&response_type=code&redirect_uri=http://localhost/exchange_token&approval_prompt=force&scope=activity:write,read
        ```
    -   After authorizing, you'll be redirected to a URL like `http://localhost/exchange_token?state=&code=YOUR_AUTH_CODE&scope=read,activity:write`.
    -   Copy the `code` value from the URL. This is your **Authorization Code**.

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
        ```

## Getting your data from Garmin

1.  Go to [https://www.garmin.com/en-US/account/datamanagement/](https://www.garmin.com/en-US/account/datamanagement/).
2.  Select "Export Your Data".
3.  Request an export of your data. This can take a while.
4.  Once your data is ready, you will receive an email with a download link.
5.  Download and unzip the file. Inside, you will find a folder named `DI_CONNECT/DI-Connect-Fitness-Uploaded-Files`. This folder contains your `.fit` files.
6.  Set the `FIT_FOLDER` in your `.env` file to the path of this folder.

## Usage

Simply run the `main.py` script:

```bash
python main.py
```

The script will:
1.  Perform a pre-sweep to move junk files.
2.  Upload the remaining FIT files to Strava.
3.  Provide summary reports for both steps.
