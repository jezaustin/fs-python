pip install -r requirements.txt
source env/bin/activate
export PYTHONPATH=`pwd`
python3 -m unittest tests/test_*.py
