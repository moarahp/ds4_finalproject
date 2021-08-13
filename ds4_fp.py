import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import io
import requests

#body
st.title("Increasing Delivery Success in the first attempt")
st.subheader("Live app of data referring to the final project of the Group 18 Data Science for all Empowerment")

@st.cache
def load_csv ():
    resp = requests.get(
        "https://ds4a-moarah-data.s3.us-east-2.amazonaws.com/ds4a_v1.parquet",
        stream=True
    )
    resp.raw.decode_content = True
    mem_fh = io.BytesIO(resp.raw.read())
    df_origin = pq.read_table(mem_fh).to_pandas()
    df_origin['pick_up_hour'] = pd.to_datetime(df_origin['route_pickup_checkout_time']).dt.hour.astype('Int64')
    return df_origin

df_2 = load_csv()

state = st.sidebar.selectbox(
    "State",
    df_2['address_state'].unique()
)

#state sucess rate chart
st.subheader("Delivery Sucess Rate by State")
sucess_state = df_2.groupby("address_state").agg({"first_delivery_attempt_binary": ["sum", "count"]})
sucess_state = sucess_state.droplevel(axis=1, level=0).reset_index()
sucess_state.set_index("address_state", inplace = True)
sucess_state.rename(columns={"sum":"success_packages", "count":"total_packages"}, inplace = True)
sucess_state["success_rate"] = round(sucess_state["success_packages"] / sucess_state["total_packages"],2)
sucess_state.sort_values(by=["success_rate"], inplace=True)
st.line_chart(sucess_state['success_rate'])

#Insucess packages per state
st.subheader("Number of insucess packages by State")
sucess_state = df_2.groupby("address_state").agg({"first_delivery_attempt_binary": ["sum", "count"]})
sucess_state = sucess_state.droplevel(axis=1, level=0).reset_index()
sucess_state.set_index("address_state", inplace = True)
sucess_state.rename(columns={"sum":"success_packages", "count":"total_packages"}, inplace = True)
sucess_state["insucess_packages"] = (sucess_state["total_packages"] - sucess_state["success_packages"])
sucess_state.sort_values(by=["insucess_packages"], inplace=True, ascending=False)
st.bar_chart(sucess_state['insucess_packages'])

st.subheader("Following data is based in the selected state:")

st.subheader("Number of packages by reasons of non-delivery")
df_3_1 = df_2[(df_2.address_state == state)&(df_2.insuccess_category !='Não ofensor')]
df_3_1 = df_3_1[['insuccess_category']]
df_3_1['packages'] = df_2.count
df_3_1= df_3_1.groupby(['insuccess_category']).count()
df_3_1= df_3_1.sort_values(by='packages', ascending=True)
st.bar_chart(df_3_1)

#Insucess packages per agencia
st.subheader("Top 15 cities by number of insucess packages")
dc_insucess = df_2[(df_2.address_state == state)]
dc_insucess = dc_insucess.groupby("address_city").agg({"first_delivery_attempt_binary": ["sum", "count"]})
dc_insucess = dc_insucess.droplevel(axis=1, level=0).reset_index()
dc_insucess.set_index("address_city", inplace = True)
dc_insucess.rename(columns={"sum":"success_packages", "count":"total_packages"}, inplace = True)
dc_insucess["insucess_packages"] = (dc_insucess["total_packages"] - dc_insucess["success_packages"])
dc_insucess["success_rate"] = round(dc_insucess["success_packages"] / dc_insucess["total_packages"],2)
dc_insucess.sort_values(by=["insucess_packages"], inplace=True, ascending=False)
st.bar_chart(dc_insucess[['success_rate']].head(15))

st.subheader("Top 15 distribution center by number of insucess packages")
dc_insucess = df_2[(df_2.address_state == state)]
dc_insucess = dc_insucess.groupby("distribution_center").agg({"first_delivery_attempt_binary": ["sum", "count"]})
dc_insucess = dc_insucess.droplevel(axis=1, level=0).reset_index()
dc_insucess.set_index("distribution_center", inplace = True)
dc_insucess.rename(columns={"sum":"success_packages", "count":"total_packages"}, inplace = True)
dc_insucess["insucess_packages"] = (dc_insucess["total_packages"] - dc_insucess["success_packages"])
dc_insucess["success_rate"] = round(dc_insucess["success_packages"] / dc_insucess["total_packages"],2)
dc_insucess.sort_values(by=["insucess_packages"], inplace=True, ascending=False)
st.write(dc_insucess[['insucess_packages','success_rate']].head(15))

#chart1
st.subheader("Top 15 companys by number of insucess packages")
dc_insucess = df_2[(df_2.address_state == state)]
dc_insucess = dc_insucess.groupby("company").agg({"first_delivery_attempt_binary": ["sum", "count"]})
dc_insucess = dc_insucess.droplevel(axis=1, level=0).reset_index()
dc_insucess.set_index("company", inplace = True)
dc_insucess.rename(columns={"sum":"success_packages", "count":"total_packages"}, inplace = True)
dc_insucess["insucess_packages"] = (dc_insucess["total_packages"] - dc_insucess["success_packages"])
dc_insucess["success_rate"] = round(dc_insucess["success_packages"] / dc_insucess["total_packages"],2)
dc_insucess.sort_values(by=["insucess_packages"], inplace=True, ascending=False)
st.write(dc_insucess[['insucess_packages','success_rate']].head(15))



