import logging
import time
import re


import paho.mqtt.client as pahoMqtt
from PythonLib.JsonUtil import JsonUtil
from PythonLib.Mqtt import MQTTHandler, Mqtt
from PythonLib.Scheduler import Scheduler
from PythonLib.DateUtil import DateTimeUtilities
from PythonLib.StringUtil import StringUtil
from PythonLib.Influx import Influx

logger = logging.getLogger('Mqtt2Influx')


class Module:
    def __init__(self) -> None:
        self.scheduler = Scheduler()
        self.influxDb = Influx('koserver.parents', database="Mqtt2Influx")
        self.mqttClient = Mqtt("koserver.iot", "/house", pahoMqtt.Client("Mqtt2Influx1", protocol=pahoMqtt.MQTTv311))

    def getScheduler(self) -> Scheduler:
        return self.scheduler

    def getInfluxDb(self) -> Influx:
        return self.influxDb

    def getMqttClient(self) -> Mqtt:
        return self.mqttClient

    def setup(self) -> None:
        self.scheduler.scheduleEach(self.mqttClient.loop, 500)

    def loop(self) -> None:
        self.scheduler.loop()


class Mqtt2Influx:

    def __init__(self, module: Module) -> None:
        self.mqttClient = module.getMqttClient()
        self.scheduler = module.getScheduler()
        self.influxDb = module.getInfluxDb()

        self.includePattern = []

    def setup(self) -> None:

        self.mqttClient.subscribeStartWithTopic("/house/", self.receiveData)
        self.scheduler.scheduleEach(self.__keepAlive, 10000)
        # self.influxDb.deleteDatabase()
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

        self.includePattern.append(re.compile('/house/basement/ess/essinfo_common/BATT/soc'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_common/LOAD/load_power'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_common/GRID/active_power'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_common/BATT/dc_power'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_common/PV/pv1_power'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_common/PV/pv2_power'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_common/PV/pv3_power'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_home/direction/.+?'))

        self.includePattern.append(re.compile('/house/basement/ess/essinfo_home/statistics/pcs_pv_total_power'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_home/statistics/batconv_power'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_home/statistics/load_power'))
        self.includePattern.append(re.compile('/house/basement/ess/essinfo_home/statistics/grid_power'))

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
                self.influxDb.writeOnChange('Mqtt', {topic: payload})

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

    Mqtt2Influx(module).setup()

    print("Mqtt2Influx is running")

    while (True):
        module.loop()
        time.sleep(0.25)


if __name__ == '__main__':
    main()
