

# Resetting the server #
## Reset the postgres database ##

  
```
python reset_db.py
python init_dataset_database.py --init
python watch_sequences.py --reset-all --noloop
```