FROM ubuntu

FROM python:latest

# set a directory for the app
WORKDIR /usr/src/dokertest

# copy all the files to the container
COPY . .

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# run the command
CMD ["python", "./bi_refresh.py"]