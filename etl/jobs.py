from pathlib import Path
from etl.transform.transformers.dummy import Dummy as DummyTransformer
from etl.transform.transformers.passthrough import Passthrough as PassthroughTransformer
from etl.transform.transformers.KNMI import KNMIWeatherStationData as KNMIWeatherStationDataTransformer
from etl.transform.transformers.bioclim import (
    BioClimFactory as BioClimTransformerFactory,
    BioClimEnums as BioClimTransformerEnums,
)
from etl.transform.transformers.opm import Vlinderstichting as VlinderstichtingTransformer
from etl.transform.transformers.tree import Amsterdam as TreeAmsterdamTransformer
from etl.transform.transformers.opm import Amsterdam as OPMAmsterdamTransformer
from etl.transform.transformers.tree import Gelderland as TreeGelderlandTransformer
from etl.transform.transformers.opm import Gelderland as OPMGelderlandTransformer
from etl.transform.transformers.soil import WURAlterra as WURAlterraTransformer
from etl.load.loaders.dummy import Dummy as DummyLoader
from etl.load.loaders.KNMI import KNMIWeatherStationLocation as KNMIWeatherStationLocationLoader
from etl.load.loaders.KNMI import KNMIWeatherStationData as KNMIWeatherStationDataLoader
from etl.load.loaders.bioclim import (
    BioClimFactory as BioClimLoaderFactory,
    BioClimEnums as BioClimLoaderEnums
)
from etl.load.loaders.opm import Vlinderstichting as VlinderStichtingLoader
from etl.load.loaders.tree import Amsterdam as TreeAmsterdamLoader
from etl.load.loaders.opm import Amsterdam as OPMAmsterdamLoader
from etl.load.loaders.tree import Gelderland as TreeGelderlandLoader
from etl.load.loaders.opm import Gelderland as OPMGelderlandLoader
from etl.load.loaders.soil import WURAlterra as WURAlterraLoader
from etl.load.loaders.geographical_unit import (
    Neighbourhood as NeighbourhoodLoader,
    Township as TownshipLoader,
    Province as ProvinceLoader,
)
from etl.load.loaders.great_tit import GreatTit as GreatTitLoader


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
    # ETLJob(name='KNMI_weather_station_data',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv'],
    #        transformer=KNMIWeatherStationDataTransformer(),
    #        loader=KNMIWeatherStationDataLoader()),
    # ETLJob(name='KNMI_weather_station_locations',
    #        gs_uris=['gs://vaa-opm/KNMI/station_locations.csv'],
    #        transformer=PassthroughTransformer(),
    #        loader=KNMIWeatherStationLocationLoader()),
    # ETLJob(name='Townships',
    #        gs_uris=['gs://vaa-opm/Geographical_units/townships.json'],
    #        transformer=PassthroughTransformer(),
    #        loader=TownshipLoader()),
    # ETLJob(name='Neighbourhoods',
    #        gs_uris=['gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=PassthroughTransformer(),
    #        loader=NeighbourhoodLoader()),
    # ETLJob(name='Provinces',
    #        gs_uris=['gs://vaa-opm/Geographical_units/provinces.csv'],
    #        transformer=PassthroughTransformer(),
    #        loader=ProvinceLoader()),
    # ETLJob(name='BIOCLIM_1',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_1),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_1)),
    # ETLJob(name='BIOCLIM_2',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_2),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_2)),
    # ETLJob(name='BIOCLIM_3',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_3),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_3)),
    # ETLJob(name='BIOCLIM_4',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_4),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_4)),
    # ETLJob(name='BIOCLIM_5',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_5),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_5)),
    # ETLJob(name='BIOCLIM_6',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_6),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_6)),
    # ETLJob(name='BIOCLIM_7',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_7),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_7)),
    # ETLJob(name='BIOCLIM_8',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_8),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_8)),
    # ETLJob(name='BIOCLIM_9',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_9),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_9)),
    # ETLJob(name='BIOCLIM_10',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_10),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_10)),
    # ETLJob(name='BIOCLIM_11',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_11),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_11)),
    # ETLJob(name='BIOCLIM_12',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_12),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_12)),
    # ETLJob(name='BIOCLIM_13',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_13),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_13)),
    # ETLJob(name='BIOCLIM_14',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_14),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_14)),
    # ETLJob(name='BIOCLIM_15',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_15),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_15)),
    # ETLJob(name='BIOCLIM_16',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_16),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_16)),
    # ETLJob(name='BIOCLIM_17',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_17),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_17)),
    # ETLJob(name='BIOCLIM_18',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_18),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_18)),
    # ETLJob(name='BIOCLIM_19',
    #        gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
    #                 'gs://vaa-opm/KNMI/station_locations.csv',
    #                 'gs://vaa-opm/Geographical_units/neighbourhoods.csv'],
    #        transformer=BioClimTransformerFactory.get_bioclim(BioClimTransformerEnums.bioclim_19),
    #        loader=BioClimLoaderFactory.get_bioclim(BioClimLoaderEnums.bioclim_19)),
    # ETLJob(name='Vlinderstichting',
    #        gs_uris=['gs://vaa-opm/Vlinderstichting/vlinderstichting_2017-2019.csv'],
    #        transformer=VlinderstichtingTransformer(),
    #        loader=VlinderStichtingLoader()),
    # ETLJob(name='Amsterdam_trees',
    #        gs_uris=['gs://vaa-opm/Local-governments/Amsterdam/bomenbestand.csv'],
    #        transformer=TreeAmsterdamTransformer(),
    #        loader=TreeAmsterdamLoader()),
    # ETLJob(name='Amsterdam_OPM',
    #        gs_uris=['gs://vaa-opm/Local-governments/Amsterdam/bomenbestand_geinfecteerd.csv'],
    #        transformer=OPMAmsterdamTransformer(),
    #        loader=OPMAmsterdamLoader()),
    # ETLJob(name='Gelderland_trees',
    #        gs_uris=['gs://vaa-opm/Local-governments/Gelderland/bomenbestand.csv'],
    #        transformer=TreeGelderlandTransformer(),
    #        loader=TreeGelderlandLoader()),
    # ETLJob(name='Gelderland_opm',
    #        gs_uris=['gs://vaa-opm/Local-governments/Gelderland/bomenbestand_geinfecteerd.csv'],
    #        transformer=OPMGelderlandTransformer(),
    #        loader=OPMGelderlandLoader()),
    # ETLJob(name='Bodemkaart_WUR_Alterra',
    #        gs_uris=['gs://vaa-opm/Bodem/bodemkaart.csv'],
    #        transformer=WURAlterraTransformer(),
    #        loader=WURAlterraLoader())
    ETLJob(name='Great_tit',
           gs_uris=['gs://vaa-opm/Predators/great_tit.csv'],
           transformer=PassthroughTransformer(),
           loader=GreatTitLoader())
]