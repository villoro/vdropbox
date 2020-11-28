# vdropbox

Utilities to read/write objects to/from dropbox

## Usage

The first thing to do is to declare the `Vdropbox` object using a token with:

```python
from vdropbox import Vdropbox
vdp = Vdropbox("my_secret")
```

Unlike the official `dropbox` python package it is not needed to have a leading `/` in all names.

### Basic functions

```python
# Check if a file exists
vdp.file_exists("my_file.txt")
vdp.file_exists("folder/my_file.txt")

# Check contents of a foler
vdp.ls("my_folder")

# Delete a file
vdp.delete("my_file.txt")
```

### Reading and writting text files

```python
data = "Hello world"

# Write a text file
vdp.write_file(data, "my_file.txt")

# Read a text file
vdp.read_file("my_file.txt")
```

> Internally it is using `oyaml` so all yamls are ordered.


### Reading and writting yamls

```python
data = {"a": 4, "b": 2}

# Write a yaml file
vdp.write_yaml(data, "my_file.yaml")

# Read a yaml file
vdp.read_yaml("my_file.yaml")
```

> Internally it is using `oyaml` so all yamls are ordered.

### Reading and writting excels with pandas

```python
import pandas as pd
# Dummy dataframe
df = pd.DataFrame(list("ABCDE"), columns=["col"])

# Write an excel file
vdp.write_excel(df, "df.xlsx")

# Read a parquet file
vdp.read_excel("df.parquet")
```

It is possible to pass keyworded arguments to the internal `pd.read_excel` or `df.to_excel` function.
For example:

```python
vdp.write_excel(df, "test.xlsx", index=False)
```

### Reading and writting parquets with pandas

```python
import pandas as pd
# Dummy dataframe
df = pd.DataFrame(list("ABCDE"), columns=["col"])

# Write a parquet file
vdp.write_parquet(df, "df.parquet")

# Read a parquet file
vdp.read_parquet("df.parquet")
```

It is possible to pass keyworded arguments to the internal `pd.read_parquet` or `df.to_parquet` function.

## Authors
* [Arnau Villoro](villoro.com)

## License
The content of this repository is licensed under a [MIT](https://opensource.org/licenses/MIT).
