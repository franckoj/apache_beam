class LeftJoin(beam.DoFn):
  def process(self, ele):
    data = ele[1]
    left,right= data['<left>'], data['<right>']
    try:
      if right:#notempty
        for i in right[0]:
          if i != '<JoinKey>':
            left[0][i]=None    
        yield left[0]
      else:
         None
    except IndexError :
      pass
