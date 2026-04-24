
```python
from core.connection import create_connection

conn = create_connection()
if conn.test_connection():
    print("Conexión exitosa a Teradata")
```
