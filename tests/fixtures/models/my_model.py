from ezt import get_source, py_model


@py_model
def df_model():

    customers = get_source("raw_customers")
    return customers.collect()
