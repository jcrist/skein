import socket
from collections import deque

import numpy as np
import skein
from bokeh.embed import components
from bokeh.layouts import gridplot
from bokeh.models.sources import AjaxDataSource
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.themes import Theme
from jinja2 import Template
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.netutil import bind_sockets
from tornado.web import Application, RequestHandler

NPOINTS = 100
INTERVAL = 1000  # ms


class SimulatedPricingData(object):
    def __init__(self):
        self.t = 0
        self.average = 100
        self.data = dict(time=deque(maxlen=NPOINTS),
                         average=deque(maxlen=NPOINTS),
                         low=deque(maxlen=NPOINTS),
                         high=deque(maxlen=NPOINTS),
                         open=deque(maxlen=NPOINTS),
                         close=deque(maxlen=NPOINTS),
                         ma=deque(maxlen=NPOINTS),
                         macd=deque(maxlen=NPOINTS),
                         macd9=deque(maxlen=NPOINTS),
                         macdh=deque(maxlen=NPOINTS),
                         color=deque(maxlen=NPOINTS))

        def conv_kernel(days):
            a = 2.0 / (days + 1)
            kernel = np.ones(days, dtype=float)
            kernel[1:] = 1 - a
            return a * np.cumprod(kernel)

        # Cached kernels for computing the EMA
        self.kernel9 = conv_kernel(9)
        self.kernel12 = conv_kernel(12)
        self.kernel26 = conv_kernel(26)

        # Initialize data
        for i in range(NPOINTS):
            self.update()

    def _ema(self, prices, kernel):
        days = len(kernel)
        if len(prices) < days:
            return prices[-1]
        return np.convolve(list(prices)[-days:], kernel, mode="valid")[0] / 0.8647

    def update(self):
        """Compute the next element in the stream, and update the plot data"""
        # Update the simulated pricing data
        self.t += 1000 / INTERVAL
        self.average *= np.random.lognormal(0, 0.04)

        high = self.average * np.exp(np.abs(np.random.gamma(1, 0.03)))
        low = self.average / np.exp(np.abs(np.random.gamma(1, 0.03)))
        delta = high - low
        open = low + delta * np.random.uniform(0.05, 0.95)
        close = low + delta * np.random.uniform(0.05, 0.95)
        color = "darkgreen" if open < close else "darkred"

        for k, point in [('time', self.t),
                         ('average', self.average),
                         ('open', open),
                         ('high', high),
                         ('low', low),
                         ('close', close),
                         ('color', color)]:
            self.data[k].append(point)

        ema12 = self._ema(self.data['close'], self.kernel12)
        ema26 = self._ema(self.data['close'], self.kernel26)
        macd = ema12 - ema26

        self.data['ma'].append(ema12)
        self.data['macd'].append(macd)

        macd9 = self._ema(self.data['macd'], self.kernel9)

        self.data['macd9'].append(macd9)
        self.data['macdh'].append(macd - macd9)


theme = Theme(json={
    'attrs': {
        'Figure': {
            'background_fill_color': None,
            'border_fill_color': None,
            'outline_line_color': '#444444'
        },
        'Axis': {
            'axis_line_color': "whitesmoke",
            'axis_label_text_color': "whitesmoke",
            'major_label_text_color': "whitesmoke",
            'major_tick_line_color': "whitesmoke",
            'minor_tick_line_color': "whitesmoke",
            'minor_tick_line_color': "whitesmoke"
        },
        'Grid': {
            'grid_line_dash': [6, 4],
            'grid_line_alpha': .3
        },
        'Title': {
            'text_color': "whitesmoke"
        }
    }
})


template = Template('''<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Pricing Dashboard</title>
        {{ resources }}
        {{ script }}
        <style>
            body {
                background: #333333;
            }
            p, h1 {
                color: whitesmoke;
                font-family: garamond;
            }
            a {
                color: steelblue;
            }
        </style>
    </head>
    <body>
        <h1>Pricing Dashboard</h1>
        <p>An example streaming dashboard showing a <a
        href="https://en.wikipedia.org/wiki/Open-high-low-close_chart">OHLC
        chart</a> with <a href="https://en.wikipedia.org/wiki/MACD">MACD
        indicator</a> from simulated market data.</p>
        <div>
        {{ div }}
        </div>
    </body>
</html>
''')


def build_html():
    """Build the html, to be served by IndexHandler"""
    source = AjaxDataSource(data_url='./data',
                            polling_interval=INTERVAL,
                            method='GET')

    # OHLC plot
    p = figure(plot_height=400,
               title='OHLC',
               sizing_mode='scale_width',
               tools="xpan,xwheel_zoom,xbox_zoom,reset",
               x_axis_type=None,
               y_axis_location="right",
               y_axis_label="Price ($)")
    p.x_range.follow = "end"
    p.x_range.follow_interval = 100
    p.x_range.range_padding = 0
    p.line(x='time', y='average', alpha=0.25, line_width=3, color='black',
           source=source)
    p.line(x='time', y='ma', alpha=0.8, line_width=2, color='steelblue',
           source=source)
    p.segment(x0='time', y0='low', x1='time', y1='high', line_width=2,
              color='black', source=source)
    p.segment(x0='time', y0='open', x1='time', y1='close', line_width=8,
              color='color', source=source, alpha=0.8)

    # MACD plot
    p2 = figure(plot_height=200,
                title='MACD',
                sizing_mode='scale_width',
                x_range=p.x_range,
                x_axis_label='Time (s)',
                tools="xpan,xwheel_zoom,xbox_zoom,reset",
                y_axis_location="right")
    p2.line(x='time', y='macd', color='darkred', line_width=2, source=source)
    p2.line(x='time', y='macd9', color='navy', line_width=2, source=source)
    p2.segment(x0='time', y0=0, x1='time', y1='macdh', line_width=6, color='steelblue',
               alpha=0.5, source=source)

    # Combine plots together
    plot = gridplot([[p], [p2]], toolbar_location="left", plot_width=1000)

    # Compose html from plots and template
    script, div = components(plot, theme=theme)
    html = template.render(resources=CDN.render(), script=script, div=div)

    return html


class IndexHandler(RequestHandler):
    """A handler to serve the index html"""
    def initialize(self, html):
        self.html = html

    def get(self):
        self.write(self.html)


class DataHandler(RequestHandler):
    """A handler to respond to data update requests"""
    def initialize(self, data):
        self.data = data

    def get(self):
        self.write({k: list(v) for k, v in self.data.items()})


if __name__ == '__main__':
    model = SimulatedPricingData()

    app = Application([
        ("/", IndexHandler, dict(html=build_html())),
        ("/data", DataHandler, dict(data=model.data)),
    ])

    # Setup the server with a dynamically chosen port
    sockets = bind_sockets(0, '')
    server = HTTPServer(app)
    server.add_sockets(sockets)

    # Determine the dynamically chosen address
    host = socket.gethostbyname(socket.gethostname())
    port = sockets[0].getsockname()[1]
    address = 'http://%s:%s' % (host, port)
    print('Listening on %r' % address)

    # Register the page with the Skein Web UI.
    # This is the only Skein-specific bit
    app = skein.ApplicationClient.from_current()
    app.ui.add_page('price-dashboard', address,
                    link_name='Price Dashboard')

    # Register a callback to update the plot every INTERVAL milliseconds
    pc = PeriodicCallback(model.update, INTERVAL)
    pc.start()

    # Start the server
    IOLoop.current().start()
