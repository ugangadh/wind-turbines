import unittest
import pandas as pd
import src.analysis.stats as stats


class TestStats(unittest.TestCase):

    def test_stats(self):
        # Given a dataframe with cleaned readings
        data = {
            'timestamp': ['2023-01-01', '2023-01-01', '2023-01-01', '2023-01-02', '2023-01-02', '2023-01-02',
                          '2023-01-02', '2023-01-02', '2023-01-02', '2023-01-02', '2023-01-02'],
            'turbine_id': [1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3],
            'wind_speed': [10, 12, 8, 15, 14, 8, 10, 11, 10, 15, 20],
            'wind_direction': [180, 185, 200, 220, 210, 120, 140, 280, 300, 180, 120],
            'power_output': [1.00, 1.20, .80, 1.50, 1.40, 1.898753, -1.22217336, -1.05931412, 15.13199601, -0.07501486,
                             0.14636453]
        }

        input_df = pd.DataFrame(data)
        input_df['timestamp'] = pd.to_datetime(input_df['timestamp'])

        # Calculate_stats
        stats_df = stats.calculate_stats(input_df)

        # Expected result based on the provided DataFrame
        expected_result = pd.DataFrame({
            'turbine_id': [1, 2, 3],
            'min_power': [1.00, 0.80, -1.22],
            'max_power': [1.20, 1.50, 15.13],
            'average': [1.10, 1.23, 2.47],
            'has_anomaly_reading': [False, False, True]
        })

        expected_result = expected_result.astype({
            "turbine_id": "int64",
            "min_power": "float64",
            "max_power": "float64",
            "average": "float64",
        }).round(2)

        # Check if the stats DataFrame is as expected
        print("Input Dataframe:")
        print(input_df)
        print("Stats Dataframe:")
        print(stats_df)
        print("Expected Dataframe:")
        print(expected_result)

        # Turbine 3 has an anomaly reading
        # Assert that the actual and expected results are equal
        pd.testing.assert_frame_equal(stats_df.reset_index(drop=True),
                                      expected_result.reset_index(drop=True))


if __name__ == '__main__':
    unittest.main()
