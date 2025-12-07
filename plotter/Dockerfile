# base on latest python file
FROM python:latest

# add our python program
ADD plotter.py ./

# install dependent libraries
RUN pip install pika
RUN pip install matplotlib
RUN pip install awkward
RUN pip install matplotlib_inline
RUN pip install  IPython
RUN pip install numpy
RUN pip install atlasopenmagic

# the command to run our program
CMD [ "python", "./plotter.py"]
