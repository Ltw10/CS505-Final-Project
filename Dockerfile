# start by pulling the python image
FROM python:3.9-alpine

# switch working directory
WORKDIR /

# copy the requirements file into the image
COPY requirements.txt requirements.txt

# copy every content from the local file to the image
COPY . .

# install the dependencies and packages in the requirements file
RUN pip install -r requirements.txt

# configure the container to run in an executed manner
ENTRYPOINT [ "python3" ]

CMD ["app.py"]