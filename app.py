import csv
import statistics
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.filechooser import FileChooserListView

class MyApp(App):
    def build(self):
        box_layout = BoxLayout(orientation='vertical')
        self.file_chooser = FileChooserListView()
        self.file_chooser.filters += (FileChooserListView.extension_filter('csv'),)
        self.file_chooser.filters -= FileChooserListView.hidden_filter()
        box_layout.add_widget(self.file_chooser)
        button = Button(text='Calculate Regression')
        button.bind(on_press=self.calculate_regression)
        box_layout.add_widget(button)
        self.regression_label = Label(text='Regression equation will be shown here.')
        box_layout.add_widget(self.regression_label)

        return box_layout

    def load_csv(self):
        if not self.file_chooser.selection:
            return
        file_path = self.file_chooser.selection[0]
        x_values = []
        y_values = []

        with open(file_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                x_values.append(float(row["X"]))
                y_values.append(float(row["Y"]))

        self.x_values = x_values
        self.y_values = y_values

    def calculate_regression(self, instance):
        if not hasattr(self, 'x_values') or not hasattr(self, 'y_values'):
            return
        x_mean = statistics.mean(self.x_values)
        y_mean = statistics.mean(self.y_values)

        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(self.x_values, self.y_values))
        denominator = sum((x - x_mean) ** 2 for x in self.x_values)

        m = numerator / denominator
        c = y_mean - m * x_mean

        self.regression_label.text = f"y = {m:.2f}x + {c:.2f}"

if __name__ == "__main__":
    MyApp().run()