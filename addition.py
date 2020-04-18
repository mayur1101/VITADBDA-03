class Calculate():
    def inputs(self,x,y):
        self.first=x
        self.second=y

class Addition(Calculate):
    def ans(self):
        z=self.first+self.second
        print("addition of",self.first,"and",self.second,"is:",z)