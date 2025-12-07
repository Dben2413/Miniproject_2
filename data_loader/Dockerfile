# base on latest python file
FROM python:latest

# add our python program
ADD data_loader.py ./

# install dependent libraries
RUN pip install pika
RUN pip install numpy
RUN pip install  matplotlib
RUN pip install  matplotlib_inline
RUN pip install  uproot
RUN pip install  awkward
RUN pip install  vector 
# RUN pip install  time 
RUN pip install  requests 
RUN pip install  pyarrow
RUN pip install  IPython
RUN pip install atlasopenmagic
RUN pip install aiohttp

CMD [ "python", "-u", "./data_loader.py"]
