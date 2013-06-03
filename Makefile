default: python

python:
	thrift -out src/interface/ --gen py interface.thrift
	echo "from interface import *" > src/interface/__init__.py

