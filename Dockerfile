FROM python:3
#WORKDIR /var/test/
COPY main.py /
COPY requirements.txt requirements.txt
#VOLUME -/Users/pavel/PycharmProjects/flightradar:/var/test/
#ENV PYTHONIOENCODING=cp1251
RUN pip3 install -r requirements.txt
CMD [ "python", "/main.py" ]