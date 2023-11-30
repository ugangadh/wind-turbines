import unittest
import pandas as pd
import src.pipeline.etl as etl


class TestCleanDataFrame(unittest.TestCase):

    def test_timestamp_cleaning(self):
        # Create a sample DataFrame with invalid timestamps
        data = {
            'timestamp': ['2022-01-01 00:00:00', 'invalid', '2022-01-03 00:00:00'],
            'turbine_id': [1, 2, 3],
            'wind_speed': [10, 15, 12],
            'wind_direction': [180, 200, 220],
            'power_output': [1.0, 1.5, 1.2]
        }
        input_df = pd.DataFrame(data)

        # Expected result after cleaning
        expected_result = pd.DataFrame({
            'timestamp': ['2022-01-01 00:00:00', '2022-01-03 00:00:00'],
            'turbine_id': [1, 3],
            'wind_speed': [10, 12],
            'wind_direction': [180, 220],
            'power_output': [1.0, 1.2]
        }).reset_index(drop=True)
        expected_result = expected_result.astype({
            "timestamp": "datetime64[ns]",
            "turbine_id": "int64",
            "wind_speed": "float64",
            "wind_direction": "int64",
            "power_output": "float64",
        })

        # Clean the DataFrame
        cleaned_df = etl.clean_data(input_df).reset_index(drop=True)

        # Check if the cleaned DataFrame is as expected
        print("Input Dataframe:")
        print(input_df)
        print("Cleaned Dataframe:")
        print(cleaned_df)
        pd.testing.assert_frame_equal(cleaned_df, expected_result)

    def test_turbine_id_cleaning(self):
        # Create a sample DataFrame with invalid turbine id
        data = {
            'timestamp': ['2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-01-03 00:00:00'],
            'turbine_id': [1, 'some_id', 3],
            'wind_speed': [10, 15, 12],
            'wind_direction': [180, 200, 220],
            'power_output': [1.0, 1.5, 1.2]
        }
        input_df = pd.DataFrame(data)

        # Expected result after cleaning
        expected_result = pd.DataFrame({
            'timestamp': ['2022-01-01 00:00:00', '2022-01-03 00:00:00'],
            'turbine_id': [1, 3],
            'wind_speed': [10, 12],
            'wind_direction': [180, 220],
            'power_output': [1.0, 1.2]
        }).reset_index(drop=True)
        expected_result = expected_result.astype({
            "timestamp": "datetime64[ns]",
            "turbine_id": "int64",
            "wind_speed": "float64",
            "wind_direction": "int64",
            "power_output": "float64",
        })

        # Clean the DataFrame
        cleaned_df = etl.clean_data(input_df).reset_index(drop=True)

        # Check if the cleaned DataFrame is as expected
        print("Input Dataframe:")
        print(input_df)
        print("Cleaned Dataframe:")
        print(cleaned_df)
        print("Expected Dataframe:")
        print(expected_result)
        pd.testing.assert_frame_equal(cleaned_df, expected_result)

    def test_imputing(self):
        # Create a sample DataFrame with missing wind direction
        data = {
            'timestamp': ['2022-01-01 00:00:00', '2022-01-03 00:00:00', '2022-01-03 00:00:00', '2022-01-03 01:00:00',
                          '2022-01-03 01:00:00'],
            'turbine_id': [1, 2, 3, 1, 4],
            'wind_speed': [10, 15, 12, 14, 11],
            'wind_direction': ['', 180, 200, 220, ''],
            'power_output': [1.0, 1.5, 1.2, 1.8, 2.2]
        }
        input_df = pd.DataFrame(data)

        # Expected result after cleaning
        expected_result = pd.DataFrame({
            'timestamp': ['2022-01-01 00:00:00', '2022-01-03 01:00:00', '2022-01-03 00:00:00', '2022-01-03 00:00:00'],
            'turbine_id': [1, 1, 2, 3],
            'wind_speed': [10, 14, 15, 12],
            'wind_direction': [220, 220, 180, 200],
            'power_output': [1.0, 1.8, 1.5, 1.2]
        }).reset_index(drop=True)
        expected_result = expected_result.astype({
            "timestamp": "datetime64[ns]",
            "turbine_id": "int64",
            "wind_speed": "float64",
            "wind_direction": "int64",
            "power_output": "float64",
        })

        # Clean the DataFrame
        cleaned_df = etl.clean_data(input_df).reset_index(drop=True)

        # Check if the cleaned DataFrame is as expected ie the turbine 1 missing wind direction is back-filled
        # and the turbine 4 reading is dropped
        print("Input Dataframe:")
        print(input_df)
        print("Cleaned Dataframe:")
        print(cleaned_df)
        print("Expected Dataframe:")
        print(expected_result)
        pd.testing.assert_frame_equal(cleaned_df, expected_result)

    def test_range_validation(self):
        # Create a sample DataFrame with out-of-range values for wind direction
        data = {
            'timestamp': ['2022-01-01 00:00:00', '2022-01-03 00:00:00', '2022-01-03 00:00:00', '2022-01-03 01:00:00',
                          '2022-01-03 01:00:00'],
            'turbine_id': [1, 2, 3, 1, 4],
            'wind_speed': [10, 15, 12, 14, 11],
            'wind_direction': [361, 180, 200, 220, -2],
            'power_output': [1.0, 1.5, 1.2, 1.8, 2.2]
        }
        input_df = pd.DataFrame(data)

        # Expected result after cleaning
        expected_result = pd.DataFrame({
            'timestamp': ['2022-01-03 01:00:00', '2022-01-03 00:00:00', '2022-01-03 00:00:00'],
            'turbine_id': [1, 2, 3],
            'wind_speed': [14, 15, 12],
            'wind_direction': [220, 180, 200],
            'power_output': [1.8, 1.5, 1.2]
        }).reset_index(drop=True)
        expected_result = expected_result.astype({
            "timestamp": "datetime64[ns]",
            "turbine_id": "int64",
            "wind_speed": "float64",
            "wind_direction": "int64",
            "power_output": "float64",
        })

        # Clean the DataFrame
        cleaned_df = etl.clean_data(input_df).reset_index(drop=True)

        # Check if the cleaned DataFrame is as expected ie the turbine 1 with wind direction 361 is removed
        # along with turbine 4 with negative value
        print("Input Dataframe:")
        print(input_df)
        print("Cleaned Dataframe:")
        print(cleaned_df)
        print("Expected Dataframe:")
        print(expected_result)
        pd.testing.assert_frame_equal(cleaned_df, expected_result)


def run_unit_tests():
    unittest.main()


if __name__ == '__main__':
    unittest.main()
