# tap-contentquo

`tap-contentquo` is a Singer tap for ContentQuo. It extracts data from ContentQuo. It leverages the ContentQuo API to pull data into your data warehouse or data lake.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.


## Installation

### 1. Adding Tap-ContentQuo to an Existing Meltano Project

To add the `tap-contentquo` to an existing Meltano project, follow these steps:

1. **Navigate to your Meltano project directory**:
  Open your terminal and change to the directory of your Meltano project.
  
   ```bash 
    cd your-meltano-project
   ```

2. **Add the Tap-ContentQuo extractor**:
   Use the `meltano add` command to add `tap-contentquo` to your project.
   
   ```bash
   meltano add extractor tap-contentquo
   ```

   or update your `meltano.yml` file with below configuration
   ```yaml
   plugins:
      extractors:
        - name: tap-contentquo
          namespace: tap_contentquo
          pip_url: git+https://github.com/degreed-data-engineering/tap-contentquo
          config:
            key: <key for ContentQuo API service>
            secret: <secret for the ContentQuo API service>
            base_url: <base url for the ContentQuo API service>
   ```

3. **Configure the Tap-ContentQuo extractor**:
   After adding the extractor, you need to configure it. You can do this interactively by running:
   
   ```bash
    meltano config tap-contentquo set --interactive
   ```
   Or, you can set the config environment variable in your .env file. For example:
   ```bash
    TAP_CONTENTQUO_KEY="your_key_here"
    TAP_CONTENTQUO_SECRET="your_secret_here"
    TAP_CONTENTQUO_API_BASE_URL="base_url"
   ```

4. **Test the Tap-ContentQuo extractor configuration**:
   To ensure everything is configured correctly, test the configuration using:
   
   ```bash
    meltano config tap-contentquo test
   ```

5. **Run the Extractor**:
   Finally, run the extractor to start pulling data from ContentQuo into your Meltano project. You can specify the target loader in the command. For example, if you're using `target-jsonl` as your loader:

   ```bash
    meltano run tap-contentquo target-jsonl
   ```

By following these steps, you will have successfully added `tap-contentquo` to your existing Meltano project, configured it with your ContentQuo API key, and started extracting data.

### 2. Install from GitHub:

```bash
pipx install git+https://github.com/degreed-data-engineering/tap-contentquo.git
```

## Configuration

### Accepted Config Options

`tap-contentquo` requires an access token to connect and authenticate with the ContentQuo APIs. This is a mandatory configurations. 

  - `key`: This is your ContentQuo key. 
  - `secret`: This is your ContentQuo secret. 

Other configurations are
  - `api_base_url`: Base url for the ContentQuo API service.

You can set this API key in your environment variables:

```bash
export TAP_CONTENTQUO_KEY=your_key_here
export TAP_CONTENTQUO_SECRET=your_secret_here
export TAP_CONTENTQUO_API_BASE_URL="base_url"
``` 

Alternatively, you can create a .env file in your project directory and add the following line:

```bash
TAP_CONTENTQUO_KEY="your_key_here"
TAP_CONTENTQUO_SECRET="your_secret_here"
TAP_CONTENTQUO_API_BASE_URL="base_url"
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Accepted Config Options

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-contentquo --about
```
Pre-Requisite tor run above command

1. Install the Tap ContentQuo: If you haven't already installed the `tap-contentquo`, you need to do so. The installation method can vary depending on whether `tap-contentquo` is a standalone tool or part of a larger framework. If it's a Python package, you might use pip to install it: 

```bash
  pipx install git+https://github.com/degreed-data-engineering/tap-contentquo.git
  ```

<!-- ### Source Authentication and Authorization -->
<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-contentquo` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-contentquo --version
tap-contentquo --help
tap-contentquo --config CONFIG --discover > ./catalog.json
```

### Executing the Tap Within A Meltano Project

Use the `meltano config` command to list the settings your extractor supports:

```bash
meltano config tap-contentquo list
```
To set the appropriate values for each setting using the `meltano config` command:

```bash
meltano config tap-contentquo set <setting> <value>
```
or 

```bash
meltano config tap-contentquo set --interactive
```

If you encounter issues or need to verify the configuration, you can use the meltano config command to test the extractor settings:

```bash
meltano config tap-contentquo test
```


## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-contentquo` CLI interface directly using `poetry run`:

```bash
poetry run tap-contentquo --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-contentquo
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-contentquo --version
# OR run a test `elt` pipeline:
meltano elt tap-contentquo target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.