FROM python:3
COPY main.py /
COPY LMPLocations.csv /
COPY requirements.txt requirements.txt /
RUN pip3 install -r requirements.txt
CMD [ "python", "/main.py" ]