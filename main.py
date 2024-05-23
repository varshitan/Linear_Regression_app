import csv
import numpy as np
import matplotlib.pyplot as plt

def read_csv(file_path):
    x = []
    y = []
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  
        for row in csv_reader:
            x.append(float(row[0]))
            y.append(float(row[1]))
    return np.array(x), np.array(y)

def linear_regression(x, y):
    x = np.vstack([x, np.ones(len(x))]).T
    theta = np.linalg.inv(x.T @ x) @ x.T @ y
    return theta

def plot_regression(x, y, theta):
    plt.scatter(x, y)
    plt.plot(np.unique(x), np.unique(x) * theta[0] + theta[1], color='red')
    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('Linear Regression')
    plt.show()

if __name__ == '__main__':
    file_path = r'C:\Users\DELL\Linear_Regression_app\Sample_Input - Sheet1.csv'  
    x, y = read_csv(file_path)
    theta = linear_regression(x, y)
    print(f'Linear regression coefficients: m = {theta[0]:.2f}, c = {theta[1]:.2f}')
    plot_regression(x, y, theta)
