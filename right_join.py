class right_join(beam.DoFn):
  def process(self, ele):
    data = ele[1]
    left,right= data['<left>'], data['<right>']
    try:
      if left:#notempty
        for i in left[0]:
          if i != '<JoinKey>':
            right[0][i]=None
        yield right[0]
      else:
        None
    except IndexError :
      pass
