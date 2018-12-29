FROM python:3

ADD ./currencyproducer /currencyproducer
ADD ./requirements.txt /

RUN pip install -r requirements.txt

CMD python -u -m currencyproducer.main ${SYMBOL} --reference ${REFERENCE:-EUR} --sleep ${SLEEP:-5} --kafka_servers ${KAFKA_SERVERS:-localhost:9092} --kafka_topic ${KAFKA_TOPIC:-kt_currencies} --rollback ${ROLLBACK:-10080}
