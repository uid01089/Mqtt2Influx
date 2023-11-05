import logging
import time
import re


import paho.mqtt.client as pahoMqtt
from PythonLib.Mqtt import MQTTHandler, Mqtt
from PythonLib.Scheduler import Scheduler
from PythonLib.DateUtil import DateTimeUtilities
from PythonLib.StringUtil import StringUtil
from PythonLib.Influx import Influx

logger = logging.getLogger('Mqtt2Influx')


class Mqtt2Influx:

    def __init__(self, mqttClient: Mqtt, scheduler: Scheduler, influxDb: Influx) -> None:
        self.mqttClient = mqttClient
        self.scheduler = scheduler
        self.influxDb = influxDb
        self.includePattern = []

    def setup(self) -> None:

        self.mqttClient.subscribeStartWithTopic("/house/", self.receiveData)
        self.scheduler.scheduleEach(self.__keepAlive, 10000)
        self.influxDb.deleteDatabase()
        self.influxDb.createDatabase()

        # self.includePattern.append(re.compile('/house/agents/FroelingP2/heartbeat'))

        self.includePattern.append(re.compile('/house/garden/automower/mower/status$'))
        self.includePattern.append(re.compile('/house/garden/automower/mower/status/plain'))
        self.includePattern.append(re.compile('/house/garden/automower/mower/battery/charge'))
        self.includePattern.append(re.compile('/house/garden/automower/mower/error/message'))
        self.includePattern.append(re.compile('/house/garden/automower/health/voltage/batt'))
        self.includePattern.append(re.compile('/house/garden/automower/mower/distance'))
        self.includePattern.append(re.compile('/house/garden/automower/mower/mode'))

        self.includePattern.append(re.compile('/house/basement/solartherm/measurement/TempSensor2'))
        self.includePattern.append(re.compile('/house/basement/solartherm/measurement/TempSensor1'))
        self.includePattern.append(re.compile('/house/basement/solartherm/measurement/TempSensor4'))
        self.includePattern.append(re.compile('/house/basement/solartherm/measurement/OperationHoursRelay1'))
        self.includePattern.append(re.compile('/house/basement/solartherm/measurement/HeatQuantity'))
        self.includePattern.append(re.compile('/house/basement/solartherm/measurement/PumpSpeedRelay1'))

        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Puffert.un'))
        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Puffert.ob'))
        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Kesseltemp'))
        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Betriebszustand'))
        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Fuellst.:'))
        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Einschub'))
        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Außentemp'))
        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Vorlauft.2'))
        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Vorlauft.1'))
        self.includePattern.append(re.compile('/house/basement/heizung/measurement/Brenn.ST'))

        self.includePattern.append(re.compile('/house/attic/vallox/measurement/IncomingTemp'))
        self.includePattern.append(re.compile('/house/attic/vallox/measurement/InsideTemp'))
        self.includePattern.append(re.compile('/house/attic/vallox/measurement/OutsideTemp'))
        self.includePattern.append(re.compile('/house/attic/vallox/measurement/ExhaustTemp'))
        self.includePattern.append(re.compile('/house/attic/vallox/measurement/FanSpeed'))
        self.includePattern.append(re.compile('/house/attic/vallox/measurement/PowerState'))
        self.includePattern.append(re.compile('/house/attic/vallox/measurement/FilterguardIndicator'))

        self.includePattern.append(re.compile('/house/garden/weatherstation/temp_c'))
        self.includePattern.append(re.compile('/house/garden/weatherstation/humidity'))
        self.includePattern.append(re.compile('/house/garden/weatherstation/wind_gust_meter_sec'))
        self.includePattern.append(re.compile('/house/garden/weatherstation/wind_avg_meter_sec'))
        self.includePattern.append(re.compile('/house/garden/weatherstation/wind_direction_deg'))
        self.includePattern.append(re.compile('/house/garden/weatherstation/rain_mm'))

        self.includePattern.append(re.compile('/house/rooms/.+?/Temperature'))
        self.includePattern.append(re.compile('/house/rooms/.+?/Humidity'))
        self.includePattern.append(re.compile('/house/rooms/.+?/TargetTemperature'))
        self.includePattern.append(re.compile('/house/rooms/.+?/heating'))

    def receiveData(self, topic: str, payloadStr: str) -> None:

        try:

            matched = False
            payload = None

            for pattern in self.includePattern:
                if pattern.match(topic):
                    matched = True
                    break

            if matched:

                # Check if the variable contains a string
                if StringUtil.isNubmer(payloadStr.strip()):
                    payload = float(payloadStr.strip())
                elif (isBool := StringUtil.isBoolean(payloadStr.strip())) is not None:
                    payload = 1 if isBool else 0
                else:
                    payload = payloadStr

                # print(topic, payload)
                self.influxDb.write('Mqtt', {topic: payload})

        except BaseException:
            logging.exception('_1_')

    def __keepAlive(self) -> None:
        self.mqttClient.publishIndependentTopic('/house/agents/Mqtt2Influx/heartbeat', DateTimeUtilities.getCurrentDateString())


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('Mqtt2Influx').setLevel(logging.DEBUG)
    logging.getLogger('PythonLib.Mqtt').setLevel(logging.INFO)

    scheduler = Scheduler()

    influxDb = Influx('koserver.parents', database="Mqtt2Influx")

    mqttClient = Mqtt("koserver.iot", "/house", pahoMqtt.Client("Mqtt2Influx1", protocol=pahoMqtt.MQTTv311))
    scheduler.scheduleEach(mqttClient.loop, 500)

    logging.getLogger('Mqtt2Influx').addHandler(MQTTHandler(mqttClient, '/house/agents/Mqtt2Influx/log'))

    Mqtt2Influx(mqttClient, scheduler, influxDb).setup()

    print("Mqtt2Influx is running")

    while (True):
        scheduler.loop()
        time.sleep(0.25)


if __name__ == '__main__':
    main()
