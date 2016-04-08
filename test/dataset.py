import uuid
import itertools

NUM_CHILDREN_PER_PING = 3
SCALAR_VALUE = 42

ping_dimensions = {
    "submission_date":   [u"20160305", u"20160607"],
    "activity_date":     [u"2016-03-02T08:10:03.503Z", u"2016-06-01T08:10:17.492Z"],
    "application":       [u"Firefox", u"Fennec"],
    "doc_type":          [u"main", u"crash"],
    "channel":           [u"nightly", u"aurora"],
    "build_version":     [u"45.0a1", u"45"],
    "build_id":          [u"20160301000000", u"20160302000000"],
    "os_name":           [u"Linux", u"Windows_NT"],
    "os_version":        [u"6.1", u"3.1.12"],
    "architecture":      [u"x86", u"x86-64"],
    "e10s":              [True, False],
    "country":           ["US", "UK"],
    "experiment_id":     [None, "displayport-tuning-nightly@experiments.mozilla.org"],
    "experiment_branch": ["control", "experiment"],
}


def generate_pings():
    # obtain a list of lists of tuples of the form (DIMENSION_NAME, POSSIBLE_DIMENSION_VALUE)
    # each list in the top-level list contains only one type of DIMENSION_NAME in its tuples
    dimension_value_pairs = (
        [(dimension, value) for value in values]
        for dimension, values in ping_dimensions.items()
    )

    # the Cartesian product contains every combination of dimension values
    # this is equivalent to a very deeply nested list
    for dimensions in itertools.product(*dimension_value_pairs):
        yield generate_payload(dict(dimensions))


def generate_payload(dimensions):
    meta = {
        u"submissionDate": dimensions["submission_date"],
        u"sampleId": 42,
        u"docType": dimensions["doc_type"],
        u"geoCountry": dimensions["country"],
    }
    application = {
        u"channel": dimensions["channel"],
        u"version": dimensions["build_version"],
        u"buildId": dimensions["build_id"],
        u"name": dimensions["application"],
    }
    child_payloads = [
        {
            "histograms": {},
            "keyedHistograms": {},
            "simpleMeasurements": {}
        }
        for i in range(NUM_CHILDREN_PER_PING)
    ]
    payload = {
        u"simpleMeasurements": {
            "uptime": SCALAR_VALUE, "addonManager": {
                u'XPIDB_parseDB_MS': SCALAR_VALUE
            }
        },
        u"histograms": {
            u"UPDATE_PING_COUNT_EXTERNAL": {
                u'bucket_count': 3,
                u'histogram_type': 4,
                u'range': [1, 2],
                u'sum': SCALAR_VALUE,
                u'values': {u'0': SCALAR_VALUE, u'1': 0}
            },
        },
        u"keyedHistograms": {
            u'SUBPROCESS_CRASHES_WITH_DUMP': {
                u'content': {
                    u'bucket_count': 3,
                    u'histogram_type': 4,
                    u'range': [1, 2],
                    u'sum': SCALAR_VALUE,
                    u'values': {u'0': SCALAR_VALUE, u'1': 0}
                },
                u'plugin': {
                    u'bucket_count': 3,
                    u'histogram_type': 4,
                    u'range': [1, 2],
                    u'sum': SCALAR_VALUE,
                    u'values': {u'0': SCALAR_VALUE, u'1': 0}
                },
                u'gmplugin': {
                    u'bucket_count': 3,
                    u'histogram_type': 4,
                    u'range': [1, 2],
                    u'sum': SCALAR_VALUE,
                    u'values': {u'0': SCALAR_VALUE, u'1': 0}
                },
            },
        },
        u"childPayloads": child_payloads,
        u"info": {
            "subsessionLength": SCALAR_VALUE,
        }
    }
    environment = {
        u"system": {
            u"os": {
                u"name": dimensions["os_name"],
                u"version": dimensions["os_version"]
            }
        },
        u"settings": {
            u"telemetryEnabled": True,
            u"e10sEnabled": dimensions["e10s"],
        },
        u"build": {
            u"version": dimensions["build_version"],
            u"buildId": dimensions["build_id"],
            u"architecture": dimensions["architecture"],
        },
        u"addons": {
            u"activeExperiment": {
                u"id": dimensions["experiment_id"],
                u"branch": dimensions["experiment_branch"],
            }
        }
    }

    return {
        u"clientId": str(uuid.uuid4()),
        u"creationDate": dimensions["activity_date"],
        u"meta": meta,
        u"application": application,
        u"payload": payload,
        u"environment": environment
    }
