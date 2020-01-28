from flask import Flask, request, Response

app = Flask(__name__)


@app.route('/test', methods=["POST"])
def test():
    print(request.get_json(force=True), type(request.get_json(force=True)))
    print('delete' not in request.get_json(force=True))
    # delete = request.get_json(force=True)['delete']
    insert = request.get_json(force=True)['insert']
    # print(delete, type(delete))

    return Response(status=200)


if __name__ == "__main__":
    app.run(debug=True)