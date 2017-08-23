from mrjob.job import MRJob

class MRMinTemperatures(MRJob):
    def makeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    def mapper(self, key, line):
        (location, date, tempType, data, x, y, z, w) = line.split(",")
        if tempType == "TMIN":
            yield location, self.makeFahrenheit(data)
    
    def reducer(self, location, minTemperatures):
        yield location, min(minTemperatures)

if __name__ == "__main__":
    MRMinTemperatures.run()
