class InnerJoin(beam.DoFn):
  def process(self,ele):
    data = ele[1]
    #replace left, right string with respectice CoGroupByKey value
    left,right= data['<left>'], data['<right>']
    if left and right:
      yield {**left[0],**right[0]}
    else:
      None
