# Log Management System using `Mongo🍃`
This is the second project for the MSc course Database Management System (M149) of the winter semester. The pronunciation can be found in the _M149-Project02.pdf_. The first project can be found [here](https://github.com/LoukasPap/log-management-system).

## 🗄️ Server
1. Create and activate a virtual environment
2. Install requirements:
```bash
pip install -r requirements.txt
```
3. Start server and navigate to http://127.0.0.1:8000/.
```bash
uvicorn main:app --reload
```

## ⚙️ Data Loading
Extract files from _logs.zip_ in a new directory called _logs_. Then, run the stand-alone script `insert_data.py`.

## ⚠️ Watch out
Make sure you have downloaded MongoDB (e.g. Community Edition) before loading the data or running any query.
