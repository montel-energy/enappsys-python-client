# EnAppSys Python Client

The Python library for the [EnAppSys](https://app.enappsys.com) platform provides a light-weight Python client to interact with EnAppSys' API services. Additionally, there is an asynchronous client for non-blocking operations.

## Installation

Supports Python 3.7+.

```bash
pip install enappsys[async,pandas]
```

The extras are optional:

- `async` required for using the `EnAppSysAsync` asynchronous client.
- `pandas` required for converting API responses to DataFrames, e.g. via `client_response.to_df()`

If you only need the synchronous client and raw responses, install without extras:

```bash
pip install enappsys
```

### Configuring credentials

Your EnAppSys username and secret are required to make API requests. You can obtain these as follows:

1. Go to any download page on EnAppSys and click **Copy API URL**.
2. In the copied URL:

    - The value after `user=` is your **username**.
    - The value after `pass=` is your **secret** (a long numeric string).


The client looks for credentials in the following order:

1. **Direct arguments** when creating the client:

    ```python
    from enappsys import EnAppSys

    client = EnAppSys(
        user="example_user",
        secret="123456789123456789123456789123456789"
    )
    ```
    
2. **Environment variables**:

    ```bash
    export ENAPPSYS_USER=example_user
    export ENAPPSYS_SECRET=123456789123456789123456789123456789
    ```

3. **Credentials file** at your home directory, the default location is: `~/.credentials/enappsys.json`:

    ```json
    {
        "user": "example_user",
        "secret": "123456789123456789123456789123456789"
    }
    ```

    You can also save and specify a custom path:

    ```python
    client = EnAppSys(credentials_file="path/to/credentials.json")
    ```


### Development

Install in editable mode:

```bash
python -m pip install -e .[dev]
```

Install the commit hooks:

```bash
pre-commit install
```
