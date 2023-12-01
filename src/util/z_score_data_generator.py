import numpy as np


def generate_zscore_series():
    # Set a seed for reproducibility
    # np.random.seed(42)

    # Generate a random series of five numbers
    series = np.random.normal(size=6)

    # Choose one value and increase it to have a Z score above 3
    index_to_increase = 3
    increase_value = 6.5 * np.std(series)  # Increase by 3.5 standard deviations

    series[index_to_increase] += increase_value

    # Print the generated series and Z scores
    print("Generated Series:", series)
    print("Z Scores:", (series - np.mean(series)) / np.std(series))

    # Given data points
    data = np.array([1.898753, -1.22217336, -1.05931412, 6.13199601, -0.07501486, 0.14636453])

    # Calculate mean and standard deviation
    mean_value = np.mean(data)
    std_deviation = np.std(data)

    # Calculate Z scores for each data point
    z_scores = (data - mean_value) / std_deviation

    # Print the Z scores
    print("Data:", data)
    print("Mean:", mean_value)
    print("Standard Deviation:", std_deviation)
    print("Z Scores:", z_scores)


if __name__ == "__main__":
    generate_zscore_series()
