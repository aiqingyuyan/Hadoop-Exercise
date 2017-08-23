from mrjob.job import MRJob

class MRMaxTemperatures(MRJob):
    def makeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    def mapper(self, _, line):
        (location, date, tempType, data, x, y, z, w) = line.split(",")
        if tempType == "TMAX":
            yield location, self.makeFahrenheit(data)
    
    def reducer(self, location, minTemperatures):
        yield location, max(minTemperatures)

if __name__ == "__main__":
    MRMaxTemperatures.run()
