import datetime as dt
import os

API_KEY = str(os.environ.get("AMPLITUDE_KEY"))
print("API_KEY from env vars:", API_KEY)
