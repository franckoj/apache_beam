from inner_join import inner_join

custome_details =  [
    {
        "CustomerID":"1",
        "Name":"Maria Anders",
        "Country":"Germany"
    },
    {
        "CustomerID":"2",
        "Name":"Ana Trujillo",
        "Country":"Mexico"
    },
    {
        "CustomerID":"3",
        "Name":"Antonio Moreno",
        "Country":"Mexico"
    }
]

order_details = [
    {
        "OrderID":"10308",
        "CustomerID":"2",
        "OrderDate":"1996-09-18"
    },
    {
        "OrderID":"10309",
        "CustomerID":"1",
        "OrderDate":"1996-09-19"
    },
    {
        "OrderID":"10310",
        "CustomerID":"77",
        "OrderDate":"1996-09-20"
    }
]

p = beam.Pipeline()

#pcollection of customer details json
P1 = (
    p
    | beam.Create(customer_details)
    | "customer_details" >> beam.Map(
          lambda x: (x["CustomerID"], x))
      )

#pcollection of order details json
P2 = (
    p
    | beam.Create(order_details)
    | "order_details" >> beam.Map(
          lambda x: (x["CustomerID"], x))
      )

#joining and un-nesting using CoGroupByKey and inner_join ParDo Tranform
innerjoin = (
    {"customer_details": P1, "order_details": P2}
    | beam.CoGroupByKey()
    | "" >> beam.ParDo(inner_join())
    | beam.Map(print)
             )

p.run()
