import pickle
from components.database.RedisStorage import RedisStorage

LOG_ASSESSMENT_TABLE = 'log_assessment'

LOG_ASSESSMENT = {
    'General log tags': {
        'average': -5,
        'azimuthal': -2,
        'bad_quality': -5,
        'best': 3,
        'calibrated': 2,
        'compensated': 1,
        'computed': -5,
        'conventional': 1,
        'corrected': 1,
        'delayed': -1,
        'enhanced': 1,
        'filtered': -1,
        'focused': 2,
        'high resolution': 2,
        'horizontal': -1,
        'image': -5,
        'memory': 2,
        'natural': 0,
        'normalized': 1,
        'ratio': -4,
        'raw': 0,
        'real-time': -2,
        'reconstructed': -4,
        'synthetic': -4,
        'smoothed': -3,
        'station log': -5,
        'theoretical': -5,
        'transmitted': 0,
        'true': 2,
        'uncorrected': -1,
        'vertical': 0
    },

    'Formation Resistivity': {
        # log properties in order of decreasing priority
        'family': [
            'Formation Resistivity',
            'Induction Resistivity',
            'Laterolog Resistivity',
            'Attenuation Resistivity',
            'Phase Shift Resistivity',
            'Dielectric Resistivity',
            'Resistivity',
            'Horizontal Formation Resistivity',
            'Vertical Formation Resistivity',
            'Normal Resistivity',
            'Electromagnetic Propagation Resistivity',
            'Flushed Zone Resistivity',
            'Lateral Resistivity',
            'Spherically Focused Resistivity'
        ],
        'logging service': [
            'WL',
            'DnM'
        ],
        'investigation': [
            'ultra_deep',
            'extra_deep',
            'deep',
            'medium',
            'medium_inverted',
            'shallow',
            'extra_shallow'
        ],
        'description tags': {
            'environmentally_compensated': 5,
            'focused': 3,
            'corrected': 3,
            'eccentering_corrected': 3,
            'blended': 2,
            'memory': 2,
            'vertical': 1,
            'matched': 1,
            'compensated': 1,
            'invariant': 0,
            'induction': 0,
            'apparent': 0,
            'lateral': 0,
            'attenuation': 0,
            'azimuth': -4,
            'eccentering': -2,
            'unfocused': -2,
            'dip': -3,
            'reconstructed': -3,
            'synthetic': -3,
            'real-time': -3,
            'uncorrected': -4,
            'average': -5,
            'background': -5,
            'image': -5
        }
    }
}


def build_best_log_tags_assessment() -> None:
    '''
    Stores rules dictionary to db
    '''
    s = RedisStorage()
    s.object_set(LOG_ASSESSMENT_TABLE, pickle.dumps(LOG_ASSESSMENT))


def read_best_log_tags_assessment() -> dict:
    '''
    Reads rules dictionary from db
    '''
    s = RedisStorage()
    if not s.object_exists(LOG_ASSESSMENT_TABLE):
        build_best_log_tags_assessment()
    return pickle.loads(s.object_get(LOG_ASSESSMENT_TABLE))


if __name__ == '__main__':
    build_best_log_tags_assessment()
