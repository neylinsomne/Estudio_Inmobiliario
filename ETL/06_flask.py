from flask import Flask, send_file

app = Flask(__name__)

@app.route('/map')
def get_map():
    # Generar la imagen (o cargarla si ya la has generado previamente)
    fig, ax = plt.subplots()
    gdf.plot(ax=ax, color='blue', edgecolor='k')
    plt.title("Datos Geoespaciales")
    plt.savefig('mapa_geoespacial.png')
    plt.close()

    # Devolver la imagen como respuesta
    return send_file('mapa_geoespacial.png', mimetype='image/png')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
