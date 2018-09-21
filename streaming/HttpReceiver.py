from pyspark.serializers import PairDeserializer, NoOpSerializer
from pyspark.sql import SparkSession
from pyspark.streaming import DStream
from pyspark.streaming import StreamingContext

session = SparkSession.builder .appName("py-streaming").getOrCreate()
sc = session.sparkContext
ssc = StreamingContext(sc,10)


def create_input_stream(ssc):
    """type sc: SparkContext """
    return sc._jvm.com.dataorc.spark.streaming.HttpStream.createInputDStream(ssc)


def convert_to_dstream(ssc,javadstream):
    ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
    stream = DStream(javadstream, ssc, ser)
    return stream


inputStream = create_input_stream(ssc._jssc)
pystream = convert_to_dstream(ssc,inputStream)
pystream.map(lambda x: x[:6]).pprint()
ssc.start()
ssc.awaitTermination(1000)









print(sc._jvm.com.dataorc.spark.receiver.SimpleReceiver.check4py4j())