FROM python:3

ADD ./currencyproducer /currencyproducer
ADD ./requirements.txt /

RUN pip install -r requirements.txt

CMD python -u -m currencyproducer.main ${SYMBOL} --reference ${REFERENCE:-EUR} --sleep ${SLEEP:-5} --kafka_host ${KAFKA_HOST:-localhost} --kafka_port ${KAFKA_PORT:-9093} --kafka_topic ${KAFKA_TOPIC:-kt_currencies} --rollback ${ROLLBACK:-10080}
