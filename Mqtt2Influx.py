import logging
from pathlib import Path
import time
import re


import paho.mqtt.client as pahoMqtt
from PythonLib.JsonUtil import JsonUtil
from PythonLib.Mqtt import MQTTHandler, Mqtt
from PythonLib.MqttConfigContainer import MqttConfigContainer
from PythonLib.Scheduler import Scheduler
from PythonLib.DateUtil import DateTimeUtilities
from PythonLib.StringUtil import StringUtil
from PythonLib.Influx import Influx

logger = logging.getLogger('Mqtt2Influx')

DEFAULT_DATABASE = "myhome"


class Module:
    def __init__(self) -> None:
        self.scheduler = Scheduler()
        self.influxDbs = {}
        self.influxDb = Influx('koserver.parents', database=DEFAULT_DATABASE)  # myhome_14d
        self.mqttClient = Mqtt("koserver.iot", "/house", pahoMqtt.Client("Mqtt2Influx", protocol=pahoMqtt.MQTTv311))
        self.config = MqttConfigContainer(self.mqttClient, "/house/agents/Mqtt2Influx/config", Path("mqtt2influx.json"), {"includePattern": []})

    def getConfig(self) -> MqttConfigContainer:
        return self.config

    def getScheduler(self) -> Scheduler:
        return self.scheduler

    def getInfluxDbs(self) -> Influx:
        return self.influxDbs

    def getMqttClient(self) -> Mqtt:
        return self.mqttClient

    def setup(self) -> None:

        self.influxDbs[DEFAULT_DATABASE] = Influx('koserver.parents', database=DEFAULT_DATABASE)
        self.influxDbs["myhome_14d"] = Influx('koserver.parents', database="myhome_14d")

        self.scheduler.scheduleEach(self.mqttClient.loop, 500)
        self.scheduler.scheduleEach(self.config.loop, 60000)

    def loop(self) -> None:
        self.scheduler.loop()


class Mqtt2Influx:

    def __init__(self, module: Module) -> None:
        self.mqttClient = module.getMqttClient()
        self.scheduler = module.getScheduler()
        self.influxDbs = module.getInfluxDbs()
        self.config = module.getConfig()

        self.includePatternTuble = []

    def __updateIncludePattern(self, config: dict) -> None:
        self.includePatternTuble = []
        for patternAndDatabase in config['includePattern']:

            pattern = patternAndDatabase["pattern"]
            database = patternAndDatabase["database"]

            self.includePatternTuble.append((re.compile(pattern), self.influxDbs[database]))

    def setup(self) -> None:

        self.config.setup()
        self.config.subscribeToConfigChange(self.__updateIncludePattern)

        self.mqttClient.subscribeStartWithTopic("/house/", self.receiveData)
        self.scheduler.scheduleEach(self.__keepAlive, 10000)

    def receiveData(self, topic: str, payloadStr: str) -> None:

        try:

            payload = None

            for patternTuble in self.includePatternTuble:

                pattern = patternTuble[0]
                db = patternTuble[1]

                if pattern.match(topic):

                    # Check if the variable contains a string
                    if StringUtil.isNubmer(payloadStr.strip()):
                        payload = float(payloadStr.strip())
                    elif (isBool := StringUtil.isBoolean(payloadStr.strip())) is not None:
                        payload = 1 if isBool else 0
                    else:
                        payload = payloadStr

                    # print(topic, payload)
                    db.writeOnChange('Mqtt', {topic: payload})
                    break

        except BaseException:
            logging.exception('_1_')

    def __keepAlive(self) -> None:
        self.mqttClient.publishIndependentTopic('/house/agents/Mqtt2Influx/heartbeat', DateTimeUtilities.getCurrentDateString())
        self.mqttClient.publishIndependentTopic('/house/agents/Mqtt2Influx/subscriptions', JsonUtil.obj2Json(self.mqttClient.getSubscriptionCatalog()))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('Mqtt2Influx').setLevel(logging.DEBUG)
    logging.getLogger('PythonLib.Mqtt').setLevel(logging.INFO)

    module = Module()
    module.setup()

    logging.getLogger('Mqtt2Influx').addHandler(MQTTHandler(module.getMqttClient(), '/house/agents/Mqtt2Influx/log'))

    print("Mqtt2Influx is running")

    Mqtt2Influx(module).setup()

    while (True):
        module.loop()
        time.sleep(0.25)


if __name__ == '__main__':
    main()
