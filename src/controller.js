const Records = require("./records.model");
const fs = require("fs");
const path = require("path");
const readline = require("readline");

const upload = async (req, res) => {
  try {
    const { file } = req;
    /* Acá va tu código! Recordá que podés acceder al archivo desde la constante file */
    if (!file) {
      return res.status(400).json({ error: "No se subio ningun archivo" });
    }

    //construyo la ruta del archivo subido temporalmente por multer
    const filePath = path.join(process.cwd(), "_temp", file.filename);

    //creo un stream para leer el archivo csv sin cargarlo completamente en memoria
    const fileStream = fs.createReadStream(filePath);

    //configuro el readline para procesar el archivo linea por linea
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity, //para manejar los saltos de linea
    });

    // bueno aca defino tamaño de lote para insertar en MongoDB y variables para almacenar registros y contar líneas
    const batchSize = 1000; //defino la cantidad de 1000 registros antes de insertarlos en la DB
    let batch = []; //inicializo un array vacio para ir acumulando los registros para el insert masivo
    let lineNumber = 0; //inicio un contador para basicamente saber en que linea del archivo estoy y para saltear la primer linea
    //del encabezado como (id,firstname..etc)

    //recorro el archivo linea por linea usando un await que me permite esperar a que cada linea este lista sin bloquear la ejecucion
    for await (const line of rl) {
      lineNumber++; //incremento el contador

      if (lineNumber === 1 && line.includes("id,firstname")) continue; //pregunto si estoy en la linea 1 (el encabezado id,firstname) si es asi paso a la siguiente linea

      //parseo la linea del archivo separada por comas con el metodo de split
      const [id, firstname, lastname, email, email2, profession] =
        line.split(",");

      //creo un objeto con los datos y lo agrego al array de batch
      batch.push({
        id: Number(id),
        firstname,
        lastname,
        email,
        email2,
        profession,
      });

      //verifico basicamente si ya tengo la cantidad especificada (1000) para insertarlos todos juntos en mongoDB (insert)
      //luego limpio el batch para seguir acumulando los siguientes registros
      if (batch.length >= batchSize) {
        await Records.insertMany(batch);
        batch = [];
      }
    }

    //me aseguro que se guardan los registros que quedaron aunque sean menos de 1000
    if (batch.length > 0) {
      await Records.insertMany(batch);
    }

    //borro el archivo temporal
    fs.unlinkSync(filePath);

    return res.status(200).json({ message: "archivo procesado correctamente" });
  } catch (error) {
    return res.status(500).json({ error: "Error procesando el archivo" });
  }
};

const list = async (_, res) => {
  try {
    const data = await Records.find({}).limit(10).lean();

    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json(err);
  }
};

module.exports = {
  upload,
  list,
};
