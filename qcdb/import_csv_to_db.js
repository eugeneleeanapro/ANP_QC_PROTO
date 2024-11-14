// const fs = require('fs');
// const csv = require('csv-parser');
// const mysql = require('mysql2');
// const path = require('path');

// // Define CSV file path using `path.join` for consistency across platforms
// const csvFilePath = path.join('C:', 'Users', 'EugeneLee', 'OneDrive - ANP ENERTECH INC', 'Desktop', 'QC_CSV.csv');

// // Create a connection to the database
// const connection = mysql.createConnection({
//     host: 'localhost',
//     user: 'root',
//     password: 'mysql',
//     database: 'qcdb'
// });

// // Function to read CSV and insert data into all MySQL tables
// function importCSVToDatabase(filePath) {
//     fs.createReadStream(filePath)
//         .pipe(csv())
//         .on('data', (row) => {
//             // Insert data into the 'icp' table
//             const icpQuery = `
//                 INSERT INTO icp (lot_number, test_date, status, Sn, Si, Ca, Cr, Cu, Zr, Fe, Na, Ni, Zn, Co)
//                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
//                 ON DUPLICATE KEY UPDATE 
//                     Sn = VALUES(Sn), Si = VALUES(Si), Ca = VALUES(Ca), Cr = VALUES(Cr), 
//                     Cu = VALUES(Cu), Zr = VALUES(Zr), Fe = VALUES(Fe), Na = VALUES(Na), 
//                     Ni = VALUES(Ni), Zn = VALUES(Zn), Co = VALUES(Co)
//             `;

//             const solidContentQuery = `
//                 INSERT INTO solid_content (lot_number, solid_content, test_date, status)
//                 VALUES (?, ?, ?, ?)
//                 ON DUPLICATE KEY UPDATE solid_content = VALUES(solid_content)
//             `;

//             const particleSizeQuery = `
//                 INSERT INTO particle_size (lot_number, particle_size, test_date, status)
//                 VALUES (?, ?, ?, ?)
//                 ON DUPLICATE KEY UPDATE particle_size = VALUES(particle_size)
//             `;

//             const viscosityQuery = `
//                 INSERT INTO viscosity (lot_number, viscosity, test_date, status)
//                 VALUES (?, ?, ?, ?)
//                 ON DUPLICATE KEY UPDATE viscosity = VALUES(viscosity)
//             `;

//             const moistureQuery = `
//                 INSERT INTO moisture (lot_number, moisture, test_date, status)
//                 VALUES (?, ?, ?, ?)
//                 ON DUPLICATE KEY UPDATE moisture = VALUES(moisture)
//             `;

//             const electricalResistanceQuery = `
//                 INSERT INTO electrical_resistance (lot_number, electrical_resistance, test_date, status)
//                 VALUES (?, ?, ?, ?)
//                 ON DUPLICATE KEY UPDATE electrical_resistance = VALUES(electrical_resistance)
//             `;

//             const magneticImpurityQuery = `
//                 INSERT INTO magnetic_impurity (lot_number, magnetic_impurity, test_date, status)
//                 VALUES (?, ?, ?, ?)
//                 ON DUPLICATE KEY UPDATE magnetic_impurity = VALUES(magnetic_impurity)
//             `;

//             // Execute all queries for each row in the CSV
//             connection.execute(icpQuery, [
//                 row.lot_number, row.test_date, row.status, row.Sn, row.Si, row.Ca, row.Cr,
//                 row.Cu, row.Zr, row.Fe, row.Na, row.Ni, row.Zn, row.Co
//             ], (err) => {
//                 if (err) console.error(`Error inserting ICP data: ${err.message}`);
//             });

//             connection.execute(solidContentQuery, [
//                 row.lot_number, row.solid_content, row.test_date, row.status
//             ], (err) => {
//                 if (err) console.error(`Error inserting Solid Content data: ${err.message}`);
//             });

//             connection.execute(particleSizeQuery, [
//                 row.lot_number, row.particle_size, row.test_date, row.status
//             ], (err) => {
//                 if (err) console.error(`Error inserting Particle Size data: ${err.message}`);
//             });

//             connection.execute(viscosityQuery, [
//                 row.lot_number, row.viscosity, row.test_date, row.status
//             ], (err) => {
//                 if (err) console.error(`Error inserting Viscosity data: ${err.message}`);
//             });

//             connection.execute(moistureQuery, [
//                 row.lot_number, row.moisture, row.test_date, row.status
//             ], (err) => {
//                 if (err) console.error(`Error inserting Moisture data: ${err.message}`);
//             });

//             connection.execute(electricalResistanceQuery, [
//                 row.lot_number, row.electrical_resistance, row.test_date, row.status
//             ], (err) => {
//                 if (err) console.error(`Error inserting Electrical Resistance data: ${err.message}`);
//             });

//             connection.execute(magneticImpurityQuery, [
//                 row.lot_number, row.magnetic_impurity, row.test_date, row.status
//             ], (err) => {
//                 if (err) console.error(`Error inserting Magnetic Impurity data: ${err.message}`);
//             });
//         })
//         .on('end', () => {
//             console.log('CSV file successfully processed.');
//             connection.end(); // Close the database connection after processing
//         })
//         .on('error', (err) => {
//             console.error(`Error reading CSV file: ${err.message}`);
//             connection.end(); // Ensure the connection closes if there's an error
//         });
// }

// // Call the function and pass the path to your CSV file
// importCSVToDatabase(csvFilePath);
