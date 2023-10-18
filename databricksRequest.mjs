import axios from 'axios'

export async function get(hostname, warehouseID, token, queryString) {

    try {

    // parameters for request
    const url = new URL(`https://${hostname}/api/2.0/sql/statements/`);
    const data = {
      "warehouse_id": warehouseID,
      "schema": "tpch",
      "statement": queryString
    }
    const headers = {
      "Content-type": "application/json",
      "Authorization": `Bearer ${token}`
    };

    // Make request
    const jsonData = await axios.post(url, data, { headers })
    .then(response => response.data)

    return jsonData

    } catch (error) {
      console.log(error)
    }

}