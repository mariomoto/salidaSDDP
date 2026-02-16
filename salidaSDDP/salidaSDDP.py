import pandas as pd
import numpy as np
import os
import psr.factory
import json
import sys
from pathlib import Path

class psr_output_summary:
    
    def __init__(self, config_file):
        f= open(config_file, 'r', encoding='utf-8')
        try:
            config = json.load(f)
        except Exception as e:
            print(f"Error loading config.json: {e}")
            config = {}
        finally:
            f.close()
        self.file_root = config.get('file_root', '')
        self.bus_list = config.get('bus_list', [])
        self.gen_list = config.get('gen_list', [])
        self.Gx_Flag = config.get('Gx_Flag', False)
        self.MgC_Flag = config.get('MgC_Flag', False)
        self.cinte_Flag = config.get('cinte_Flag', False)
        self.MB_Flag = config.get('MB_Flag', False)
        self.YB_Flag = config.get('YB_Flag', False)
        self.YH_Flag = config.get('YH_Flag', False)
        self.MH_Flag = config.get('MH_Flag', False)
        self.Quant_Flag = config.get('Quant_Flag', False)
        self.Balance_Flag = config.get('Balance_Flag', False)
        self.Gx_H_Flag = config.get('Gx_H_Flag', False)
        self.Gx_units_Flag = config.get('Gx_units_Flag', False)
        self.FactorS = config.get('FactorS', False)
        self.parquet_flag = config.get('parquet_flag', False)
        self.parquet_stack = config.get('parquet_stack', False)
        self.block_dict = {i + 1: "M" if i < 8 else "D" if i < 18 else "N" for i in range(24)}

    def save_parquet(self, df, hour_flag, 
                    bus_map_file, skiprows, encoding, 
                    code_col, value_name, parquet_file, print_msg):
        try:
            bus_map = pd.read_csv(os.path.join(self.file_root, bus_map_file), skiprows=skiprows, encoding=encoding)
            if bus_map_file == "bus_conections.csv":
                bus_map.loc[bus_map["!Type code"] == 1, " Name"] = "HID." + bus_map.loc[bus_map["!Type code"] == 1, " Name"]
        except Exception:
            bus_map = {}
        parquet = df.copy()
        # print(parquet)
        parquet = parquet.reset_index()
        if hour_flag:
            parquet['date'] = pd.to_datetime(parquet[['year', 'month', 'day']].astype(str).agg('-'.join, axis=1)).dt.strftime('%Y-%m-%d')
            parquet = parquet.drop(columns=['year', 'month', 'day'])
        else:
            parquet['date'] = pd.to_datetime(parquet[['year', 'month']].astype(str).agg('-'.join, axis=1) + '-01').dt.strftime('%Y-%m-%d')
            parquet = parquet.drop(columns=['year', 'month'])
        try:
            parquet = parquet.set_index(['date', 'hour', 'scenario', "block"])
        except Exception:
            try:
                parquet = parquet.set_index(['date', 'hour', 'scenario'])
            except Exception:
                parquet = parquet.set_index(['date', 'block'])
        name_col = " Name" if " Name" in bus_map.columns else ("Name" if "Name" in bus_map.columns else None)
        if isinstance(bus_map, pd.DataFrame) and name_col and code_col in bus_map.columns:
            mapping = bus_map.set_index(name_col)[code_col].astype(str).str.strip().to_dict()
            parquet.rename(columns=mapping, inplace=True)
        parquet.columns.name = "Code"
        if self.parquet_stack:
            parquet = parquet.stack()
        parquet.name = value_name
        parquet.reset_index().to_parquet(
            os.path.join(self.file_root, parquet_file), 
            engine='pyarrow', 
            index=False,
            compression='snappy'  # Add compression to reduce memory
        )
        del parquet
        print(print_msg)

    def psr_data_reader(self, file_name):
        try:
            df = psr.factory.load_dataframe(file_name)
            df = df.to_pandas()
            return df
        except Exception as e:
            error_str = str(e)
            filename = Path(error_str).name.replace('"', '')
            print(f"psr_data_reader: {error_str[:11]} {filename}")
            return pd.DataFrame()

    def calculate_quantiles(self, df, stack_level):
        df_unstacked = df.unstack(level=stack_level).stack(level=0, future_stack=True)
        quantiles = pd.DataFrame(index=df_unstacked.index, columns=["Max", "Q3", "Mean", "Q1", "Min"])
        quantiles["Min"] = df_unstacked.quantile(0, axis=1)
        quantiles["Q1"] = df_unstacked.quantile(0.2, axis=1)
        quantiles["Mean"] = df_unstacked.quantile(0.5, axis=1)
        quantiles["Q3"] = df_unstacked.quantile(0.8, axis=1)
        quantiles["Max"] = df_unstacked.quantile(1, axis=1)
        return quantiles

    def MgC_resume(self, file_name):
        print("Leyendo Archivo CMg ...")
        df = self.psr_data_reader(os.path.join(self.file_root, file_name))
        df = df.reset_index()
        df["day"] = np.ceil(df["hour"] / 24).astype(int)
        df["hour"] = np.tile(np.arange(1, 25), len(df) // 24 + 1)[:len(df)]
        df = df.set_index(["year","month","day","scenario", "hour"])
        df["block"] = df.index.get_level_values('hour').map(self.block_dict)
        df = df.reset_index()
        df = df.set_index(["year","month","day","scenario", "hour","block"])

        if self.parquet_flag:
            self.save_parquet(df, True, 
                        "dbus.csv", 1, None, 
                        "!Code", "CMg [USD/MWh]", "CMg.parquet", "CMg.parquet Guardado.")

            # CMg mensual - bloque
            if self.MB_Flag:
                print("Calculando CMg mensual-bloque...")
                df_M = df.groupby(level=['year', 'month', 'block']).mean()
                with pd.ExcelWriter(os.path.join(self.file_root,"cmgbus_MB.xlsx"), engine='xlsxwriter') as writer:
                    df_M[self.bus_list].T.stack(level = 2).to_excel(writer,sheet_name = "CMg mensual-bloque")

            if self.YH_Flag:
                print("Calculando CMg anual-hora...")
                df_YH = df.groupby(level=['year', 'hour']).mean()
                with pd.ExcelWriter(os.path.join(self.file_root,"cmgbus_YH.xlsx"), engine='xlsxwriter') as writer:
                    df_YH[self.bus_list].to_excel(writer,sheet_name = "CMg anual-hora")
            
            if self.MH_Flag:
                print("Calculando CMg mensual-hora...")
                df_MH = df.groupby(level=['year', 'month', 'hour']).mean()
                with pd.ExcelWriter(os.path.join(self.file_root,"cmgbus_MH.xlsx"), engine='xlsxwriter') as writer:
                    df_MH[self.bus_list].to_excel(writer,sheet_name = "CMg mensual-hora")

            # CMg anual - bloque
            if self.YB_Flag:
                print("Calculando CMg anual-bloque...")
                df_YB = df.groupby(level=['year', 'block']).mean()
                # Agregar promedio por año de todos los elementos en "block"
                df_year_avg = df.groupby(level=['year']).mean()[self.bus_list]
                avg_index = pd.MultiIndex.from_tuples([(year, 'average') for year in df_year_avg.index], names=['year', 'block'])
                df_avg = pd.DataFrame(df_year_avg.values, index=avg_index, columns=bus_list)
                df_YB = pd.concat([df_YB, df_avg])
                with pd.ExcelWriter(os.path.join(self.file_root, "cmgbus_YB.xlsx"), engine='xlsxwriter') as writer:
                    df_YB[self.bus_list].T.stack(level=1, future_stack=True).to_excel(writer, sheet_name="CMg anual-bloque")

            # CMg anual - mensual - Hr - cuartiles
            if self.Quant_Flag:
                print("Calculando CMg cuantiles...")
                df_quant_YMH = self.calculate_quantiles(df,[2,3])
                df_quant_YH = self.calculate_quantiles(df,[1,2,3])
                with pd.ExcelWriter(os.path.join(self.file_root,"cmgbus_quant_YMH.xlsx"), engine='xlsxwriter') as writer:
                    for bus in self.bus_list:
                        idx = df_quant_YMH.index.get_level_values(None).isin([bus])
                        df_quant_YMH.loc[idx].reset_index().to_excel(writer, sheet_name=bus)
                with pd.ExcelWriter(os.path.join(self.file_root,"cmgbus_quant_YH.xlsx"), engine='xlsxwriter') as writer:
                    for bus in self.bus_list:
                        idx = df_quant_YH.index.get_level_values(None).isin([bus])
                        df_quant_YH.loc[idx].reset_index().to_excel(writer, sheet_name=bus)

    def Gx_resume(self, file_names):
        print("Leyendo Archivo Gx...")
        dfs = []
        for file_name in file_names:
            try:
                file_path = os.path.join(self.file_root, file_name)
                df = self.psr_data_reader(file_path)
                if 'gerhid' in file_name.lower():
                    df.columns = [f"HID.{col}" for col in df.columns]
                df = df.reset_index()
                df["day"] = np.ceil(df["hour"] / 24).astype(int)
                df["hour"] = np.tile(np.arange(1, 25), len(df) // 24 + 1)[:len(df)]
                df = df.set_index(["year","month","day","scenario", "hour"])
                if 'gerbat' in file_name.lower():
                    df /= 1000
                dfs.append(df)
            except Exception as e:
                continue
        df = pd.concat(dfs, axis=1, sort=False)
        if self.parquet_flag:
            self.save_parquet(df, True, 
                "bus_conections.csv", 2, 'latin1', 
                "Code", "Generation [GWh]", "Gx.parquet", "Gx.parquet Guardado.")
        
        # Generación por unidad
        if self.Gx_units_Flag:
            if self.Quant_Flag:
                print("Calculando Gx por unidad cuantiles...")
                df_quant_YH = self.calculate_quantiles(df,[1,2,3])
                df_quant_MH = self.calculate_quantiles(df,[2,3])
                with pd.ExcelWriter(os.path.join(self.file_root,"units_quant_YMH.xlsx"), engine='xlsxwriter') as writer:
                    for unit in self.gen_list:
                        idx = df_quant_MH.index.get_level_values(None).isin([unit])
                        df_quant_MH.loc[idx].reset_index().to_excel(writer, sheet_name=unit)
                with pd.ExcelWriter(os.path.join(self.file_root,"units_quant_YH.xlsx"), engine='xlsxwriter') as writer:
                    for unit in self.gen_list:
                        idx = df_quant_YH.index.get_level_values(None).isin([unit])
                        df_quant_YH.loc[idx].reset_index().to_excel(writer, sheet_name=unit)

        # Generacion por tecnologia
        df = df.T.groupby(lambda col: str(col)[:3].upper()).sum().T
        df = df.stack().reset_index()
        df["Tech"] = df["level_5"].str.slice(0,3)
        df = df.set_index(["year","month","day","scenario", "hour","Tech","level_5"])
        df = pd.pivot_table(df, values=0, index=["year","month","day","scenario", "hour"], columns="Tech")

        if self.Balance_Flag:
            print("Calculando balance Generacion...")
            gx_balance_Y = df.groupby(level = ["year","month","day", "hour"]).mean()
            gx_balance_Y = gx_balance_Y.groupby(level = ["year"]).sum()/1000
            gx_balance_Y.to_csv(os.path.join(self.file_root,f"gx_balance_Y.csv"))
            print("Generation balance saved.")

        if self.Gx_H_Flag:
            print("Calculando Gx por hora...")
            Gx_H_M = df.groupby(level = ["year","month","day", "hour"]).mean()
            Gx_H_M = Gx_H_M.groupby(level= ["year","month","hour"]).sum()
            Gx_H_Y = Gx_H_M.groupby(level = ["year","hour"]).sum()
            Pw_H_M = df.groupby(level = ["year","month","hour"]).mean()
            Pw_H_Y = df.groupby(level = ["year","hour"]).mean()
            with pd.ExcelWriter(os.path.join(self.file_root,"gx_hourly.xlsx"), engine='xlsxwriter') as writer:
                Gx_H_M.reset_index().to_excel(writer,sheet_name = "GWh mensual-hora",index=False)
                Gx_H_Y.reset_index().to_excel(writer,sheet_name = "GWh anual-hora", index = False)
                Pw_H_M.reset_index().to_excel(writer,sheet_name = "MW mensual-hora", index=False)
                Pw_H_Y.reset_index().to_excel(writer,sheet_name = "MW anual-hora", index=False)
            print("Gx por hora guardado.")

        if self.Quant_Flag:
            print("Calculando Gx quantiles...")
            df_quant_YH = self.calculate_quantiles(df,[1,2,3])
            df_quant_MH = self.calculate_quantiles(df,[2,3])
            with pd.ExcelWriter(os.path.join(self.file_root,f"gx_quant.xlsx"), engine='xlsxwriter') as writer:
                for tech in df.columns:
                    idx = df_quant_YH.index.get_level_values("Tech").isin([tech])
                    df_quant_YH.loc[idx].reset_index().to_excel(writer, sheet_name=tech)
            print("Cuantiles Gx guardados.")

    def TSL_resume(self, file_name):
        df = pd.read_excel(os.path.join(self.file_root,file_name))
        print(df)
        df = df.set_index(["Year","Month","Hour","Hour total"])
        if self.Quant_Flag:
            df_quant_YH = self.calculate_quantiles(df,[0,1,3])
            df_quant_MH = self.calculate_quantiles(df,[0,3])
            with pd.ExcelWriter(os.path.join(self.file_root,f"tsl_quant.xlsx"), engine='xlsxwriter') as writer:
                for col in df.columns:
                    idx = df_quant_MH.index.get_level_values(None).isin([col])
                    df_quant_MH.loc[idx].reset_index().to_excel(writer, sheet_name=col)

    def generic_parquet_export(self, file_name, hour_flag,
        bus_map_file, skiprows, encoding, 
        code_col, value_name, parquet_file):
        print(f"Leyendo archivo {file_name}...")
        df = self.psr_data_reader(os.path.join(self.file_root, file_name))
        # print(df)
        if hour_flag:
            df = df.reset_index()
            df["day"] = np.ceil(df["hour"] / 24).astype(int)
            df["hour"] = np.tile(np.arange(1, 25), len(df) // 24 + 1)[:len(df)]
            df = df.set_index(["year","month","day","scenario", "hour"])
        self.save_parquet(df, hour_flag, 
            bus_map_file, skiprows, encoding,
            code_col, value_name, parquet_file, f"{parquet_file} Guardado.",)

### MAIN CODE ###
pos = psr_output_summary("config.json")

if pos.MgC_Flag:
    pos.MgC_resume("cmgbus.hdr")
if pos.Gx_Flag:
    pos.Gx_resume(["gerter.hdr","gerhid.hdr","gergnd.hdr","gerbat.hdr"])
if pos.cinte_Flag:
    pos.generic_parquet_export("cinte1.hdr", False, 
        "bus_conections.csv", 2, 'latin1', 
        "Code", "CV [USD/MWh]", "Cinte1.parquet")
