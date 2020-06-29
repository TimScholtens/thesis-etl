from pathlib import Path
from etl.transform.transformers.dummy import Dummy as DummyTransformer
from etl.transform.transformers.passthrough import Passthrough as PassthroughTransformer
from etl.transform.transformers.KNMI import KNMIWeatherStationData as KNMIWeatherStationDataTransformer
from etl.transform.transformers.bioclim import BioClim_1 as BioClim_1_Transformer
from etl.transform.transformers.bioclim import BioClim_2 as BioClim_2_Transformer
from etl.transform.transformers.bioclim import BioClim_4 as BioClim_4_Transformer
from etl.transform.transformers.bioclim import BioClim_5 as BioClim_5_Transformer
from etl.transform.transformers.bioclim import BioClim_6 as BioClim_6_Transformer
from etl.transform.transformers.bioclim import BioClim_7 as BioClim_7_Transformer
from etl.transform.transformers.bioclim import BioClim_8 as BioClim_8_Transformer
from etl.transform.transformers.bioclim import BioClim_9 as BioClim_9_Transformer
from etl.transform.transformers.bioclim import BioClim_10 as BioClim_10_Transformer
from etl.transform.transformers.bioclim import BioClim_11 as BioClim_11_Transformer
from etl.transform.transformers.bioclim import BioClim_12 as BioClim_12_Transformer
from etl.transform.transformers.bioclim import BioClim_13 as BioClim_13_Transformer
from etl.transform.transformers.bioclim import BioClim_14 as BioClim_14_Transformer
from etl.transform.transformers.bioclim import BioClim_16 as BioClim_16_Transformer
from etl.transform.transformers.bioclim import BioClim_17 as BioClim_17_Transformer
from etl.transform.transformers.bioclim import BioClim_18 as BioClim_18_Transformer
from etl.transform.transformers.bioclim import BioClim_19 as BioClim_19_Transformer
from etl.transform.transformers.vlinderstichting import Vlinderstichting as VlinderstichtingTransformer
from etl.load.loaders.township import Township as TownshipLoader
from etl.load.loaders.dummy import Dummy as DummyLoader
from etl.load.loaders.KNMI import KNMIWeatherStationLocation as KNMIWeatherStationLocationLoader
from etl.load.loaders.KNMI import KNMIWeatherStationData as KNMIWeatherStationDataLoader
from etl.load.loaders.bioclim import BioClim_1 as BioClim_1_Loader
from etl.load.loaders.bioclim import BioClim_2 as BioClim_2_Loader
from etl.load.loaders.bioclim import BioClim_4 as BioClim_4_Loader
from etl.load.loaders.bioclim import BioClim_5 as BioClim_5_Loader
from etl.load.loaders.bioclim import BioClim_6 as BioClim_6_Loader
from etl.load.loaders.bioclim import BioClim_7 as BioClim_7_Loader
from etl.load.loaders.bioclim import BioClim_8 as BioClim_8_Loader
from etl.load.loaders.bioclim import BioClim_9 as BioClim_9_Loader
from etl.load.loaders.bioclim import BioClim_10 as BioClim_10_Loader
from etl.load.loaders.bioclim import BioClim_11 as BioClim_11_Loader
from etl.load.loaders.bioclim import BioClim_12 as BioClim_12_Loader
from etl.load.loaders.bioclim import BioClim_13 as BioClim_13_Loader
from etl.load.loaders.bioclim import BioClim_14 as BioClim_14_Loader
from etl.load.loaders.bioclim import BioClim_16 as BioClim_16_Loader
from etl.load.loaders.bioclim import BioClim_17 as BioClim_17_Loader
from etl.load.loaders.bioclim import BioClim_18 as BioClim_18_Loader
from etl.load.loaders.bioclim import BioClim_19 as BioClim_19_Loader
from etl.load.loaders.vlinderstichting import Vlinderstichting as VlinderStichtingLoader


class ETLJob:
    ETL_BASE_DIRECTORY = Path.cwd() / 'static' / 'etl'
    EXTRACT_DIRECTORY = ETL_BASE_DIRECTORY / 'extract'
    TRANSFORM_DIRECTORY = ETL_BASE_DIRECTORY / 'transform'

    def __init__(self, name,
                 gs_uris,
                 transformer=DummyTransformer(),
                 loader=DummyLoader()):
        """
        :param name: name of etl config item.
        :param gs_uris: list of file locations within google cloud storage . eg.
        eg. [gs://vaa-opm/knmi/weather_station_data.json, gs://vaa-opm/knmi/weather_station_locations.json']
        :param transformer: transformer class to use for transforming data.
        :param loader: loader class to use for loading data into database.
        """
        self._name = name
        self._source_location = gs_uris
        self._transformer = transformer
        self._loader = loader

    @property
    def name(self):
        return self._name

    @property
    def gs_uris(self):
        return self._source_location

    @property
    def extract_location(self):
        """Directory where extracted data will be saved"""
        return ETLJob.EXTRACT_DIRECTORY / f'{self._name}'

    @property
    def transform_location(self):
        """Directory where transformed data will be saved"""
        return ETLJob.TRANSFORM_DIRECTORY / f'{self._name}'

    @property
    def transformer(self):
        return self._transformer

    @property
    def loader(self):
        return self._loader


# noinspection PyTypeChecker
ETL_JOBS = [
    ETLJob(name='KNMI_weather_station_data',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv'],
           transformer=KNMIWeatherStationDataTransformer(),
           loader=KNMIWeatherStationDataLoader()),
    ETLJob(name='KNMI_weather_station_locations',
           gs_uris=['gs://vaa-opm/KNMI/station_locations.csv'],
           transformer=PassthroughTransformer(),
           loader=KNMIWeatherStationLocationLoader()),
    ETLJob(name='Townships',
           gs_uris=['gs://vaa-opm/Townships/townships.json'],
           transformer=PassthroughTransformer(),
           loader=TownshipLoader()),
    ETLJob(name='BIOCLIM_1',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_1_Transformer(),
           loader=BioClim_1_Loader()),
    ETLJob(name='BIOCLIM_2',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_2_Transformer(),
           loader=BioClim_2_Loader()),
    ETLJob(name='BIOCLIM_4',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_4_Transformer(),
           loader=BioClim_4_Loader()),
    ETLJob(name='BIOCLIM_5',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_5_Transformer(),
           loader=BioClim_5_Loader()),
    ETLJob(name='BIOCLIM_6',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_6_Transformer(),
           loader=BioClim_6_Loader()),
    ETLJob(name='BIOCLIM_7',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_7_Transformer(),
           loader=BioClim_7_Loader()),
    ETLJob(name='BIOCLIM_8',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_8_Transformer(),
           loader=BioClim_8_Loader()),
    ETLJob(name='BIOCLIM_9',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_9_Transformer(),
           loader=BioClim_9_Loader()),
    ETLJob(name='BIOCLIM_10',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_10_Transformer(),
           loader=BioClim_10_Loader()),
    ETLJob(name='BIOCLIM_11',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_11_Transformer(),
           loader=BioClim_11_Loader()),
    ETLJob(name='BIOCLIM_12',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_12_Transformer(),
           loader=BioClim_12_Loader()),
    ETLJob(name='BIOCLIM_13',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_13_Transformer(),
           loader=BioClim_13_Loader()),
    ETLJob(name='BIOCLIM_14',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_14_Transformer(),
           loader=BioClim_14_Loader()),
    ETLJob(name='BIOCLIM_16',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_16_Transformer(),
           loader=BioClim_16_Loader()),
    ETLJob(name='BIOCLIM_17',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_17_Transformer(),
           loader=BioClim_17_Loader()),
    ETLJob(name='BIOCLIM_18',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_18_Transformer(),
           loader=BioClim_18_Loader()),
    ETLJob(name='BIOCLIM_19',
           gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                    'gs://vaa-opm/KNMI/station_locations.csv',
                    'gs://vaa-opm/Townships/townships.json'],
           transformer=BioClim_19_Transformer(),
           loader=BioClim_19_Loader()),
    ETLJob(name='Vlinderstichting',
           gs_uris=['gs://vaa-opm/Vlinderstichting/epr_20200521.csv'],
           transformer=VlinderstichtingTransformer(),
           loader=VlinderStichtingLoader())
]
