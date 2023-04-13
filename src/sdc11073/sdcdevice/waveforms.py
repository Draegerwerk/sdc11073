import math
import itertools


def sinus(min_value, max_value, samples):
    delta = 2*math.pi/samples
    _values = [math.sin(i*delta) for i in range(samples)]  # -1 ... +1
    values = [(n+1)/2.0*(max_value-min_value) + min_value for n in _values]  # min ... max
    return values


def sawtooth(min_value, max_value, samples):
    delta = (max_value - min_value)/float(samples)
    values = [min_value + i*delta for i in range(samples)]
    return values


def triangle(min_value, max_value, samples):
    min_value = float(min_value)
    max_value = float(max_value)
    delta = (max_value - min_value)/float(samples) *2
    samples_cnt = int(samples/2)
    values = [min_value + i*delta for i in range(samples_cnt)]+ [max_value - i*delta for i in range(samples_cnt)]
    return values


    
class _WaveformGeneratorBase(object):
    def __init__(self, values_generator, min_value, max_value, waveformperiod, sampleperiod):
        if sampleperiod >= waveformperiod:
            raise ValueError('please choose a waveformperiod >> sampleperiod. currently use have wp={}, sp={}'.format(waveformperiod, sampleperiod))
        self.sampleperiod = sampleperiod
        samples = int(waveformperiod / sampleperiod)
        values = values_generator(min_value, max_value, samples)     
        wf_with_startflag = [ (v, i==0) for i, v in  enumerate(values)]
        self._values = wf_with_startflag
        self._generator = itertools.cycle(self._values)


    def nextSamples(self, count):
        return [next(self._generator) for i in range(count)]

    

class TriangleGenerator(_WaveformGeneratorBase):
    def __init__(self, min_value, max_value, waveformperiod, sampleperiod):
        super(TriangleGenerator, self).__init__(triangle, min_value, max_value, waveformperiod, sampleperiod)



class SawtoothGenerator(_WaveformGeneratorBase):
    def __init__(self,  min_value, max_value, waveformperiod, sampleperiod):
        super(SawtoothGenerator, self).__init__(sawtooth,  min_value, max_value, waveformperiod, sampleperiod)



class SinusGenerator(_WaveformGeneratorBase):
    def __init__(self,  min_value, max_value, waveformperiod, sampleperiod):
        super(SinusGenerator, self).__init__(sinus, min_value, max_value, waveformperiod, sampleperiod)
