# Training a LSTM model and saving the model to an .h5 file including a seperated .save file for the MinMaxScaler data

## 1) Purpose of the script
`````
The scripts purpose is it to train and save a LSTM model. In order to enable the possibility of having different data sets the script can be configured for each data such (see 4) Configuration options). 
`````
## 2) Usage
`````
Localed the data set on which the alorithem shall be trained in the ./engine/data folder. 
Open the processing_config.json file (./engine/.config). In here you can change the dataframePath so it matched the file you want to use.
Next change the 'date-col' to the column name in which the date of transaction is specified and the 'target-col' to the column holding the sales data. 
The data can be grouped differently (W, W-MON, M, M-MON -> see 4) Configuration options) there for change the field variable 'group-by' to the option best for the outupt. (Depending on the amount of data or the diversity group by weeks or month can change the prediction dramticly).
Finally if you run the code in the cli (via python3 LSTM_train_save.py $1 $2) the first argument ($1) specifise the file name of the saved model and the second argument ($2) specifise the file name for the MinMaxScaler information.
These both files will be saved in the ./saved_model_data folder (if you use the same file name for either the model or the scaler the files WILL be overwritten).
Once the model is trained and model as well as scaler are saved they can be refferenced in the script running the predictions on the platform (see README infrastructure)

`````
###### Usage in script formate
`````
# set configurations
{
    "lstm-script": {
        "dataFrame-path": "path to data set",
        "date-col": "column name with transaction date",
        "target-col": "column name with sales transactions",
        "group-by": "W / W-MON / M / M-MON"
    }
}
`````

`````
# cli command to run the script
python3 LSTM_train_save.py model_file_name scaler_file_name
`````

## 3) Script protocol
`````
`````
## 4) Configuration options
`````
`````

# TODO:
`````
- Create shell script for running the script
- Create setup for python script plus requirements file
`````