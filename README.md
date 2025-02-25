# vdropbox

Utilities to read/write objects to/from Dropbox.

## 🚀 Usage

The first step is to declare the `Vdropbox` object using a token:

```python
from vdropbox import Vdropbox
vdp = Vdropbox("my_secret")
```

Unlike the official `dropbox` Python package, it is **not required** to have a leading `/` in all file names.

> [!NOTE]
> Ensure your Dropbox token has the necessary permissions.

### 🛠 Custom Logger

You can pass a custom logger to `Vdropbox` if you want to control logging behavior:

```python
import logging
from vdropbox import Vdropbox

logger = logging.getLogger("my_logger")
vdp = Vdropbox("my_secret", logger=logger)
```

> [!TIP]
> Using a custom logger allows you to integrate `Vdropbox` logs into your existing logging setup.

## 📁 Basic Functions

```python
# Check if a file exists
vdp.file_exists("my_file.txt")
vdp.file_exists("folder/my_file.txt")

# Check contents of a folder
vdp.ls("my_folder")

# Delete a file
vdp.delete("my_file.txt")
```

> [!WARNING]
> Deleting a file is irreversible!

## 📝 Reading and Writing Text Files

```python
data = "Hello world"

# Write a text file
vdp.write_file(data, "my_file.txt")

# Read a text file
vdp.read_file("my_file.txt")
```

> [!NOTE]
> The default encoding is UTF-8.

## 📜 Reading and Writing YAML Files

```python
data = {"a": 4, "b": 2}

# Write a YAML file
vdp.write_yaml(data, "my_file.yaml")

# Read a YAML file
vdp.read_yaml("my_file.yaml")
```

> [!TIP]
> Internally, it uses `oyaml`, so all YAML files maintain their order.

## 📊 Reading and Writing Excel Files with Pandas

```python
import pandas as pd
# Create a dummy DataFrame
df = pd.DataFrame(list("ABCDE"), columns=["col"])

# Write an Excel file
vdp.write_excel(df, "df.xlsx")

# Read an Excel file
df = vdp.read_excel("df.xlsx")
```

> [!TIP]
> You can pass keyword arguments to `pd.read_excel` or `df.to_excel`.

Example:

```python
vdp.write_excel(df, "test.xlsx", index=False)
```

## 🔹 Reading and Writing Parquet Files with Pandas

```python
import pandas as pd
# Create a dummy DataFrame
df = pd.DataFrame(list("ABCDE"), columns=["col"])

# Write a Parquet file
vdp.write_parquet(df, "df.parquet")

# Read a Parquet file
df = vdp.read_parquet("df.parquet")
```

> [!TIP]
> You can pass keyword arguments to `pd.read_parquet` or `df.to_parquet`.

## 👥 Authors

- [Arnau Villoro](https://villoro.com)

## 📜 License

The content of this repository is licensed under [MIT](https://opensource.org/licenses/MIT).

