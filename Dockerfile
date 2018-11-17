FROM python:3

ADD ./src /
ADD ./requirements.txt /

RUN pip install -r requirements.txt

CMD python -u ./main.py --symbol ${SYMBOL} --reference ${REFERENCE:-EUR} --sleep ${SLEEP:-5} --kafka_host ${KAFKA_HOST} --kafka_port ${KAFKA_PORT:-9093} --kafka_topic ${KAFKA_TOPIC:-kt_currencies} --rollback ${ROLLBACK:-10080}
