import { createReadStream } from "fs";
import { createInterface } from "readline";

async function countCSVRows(filePath) {
  const fileStream = createReadStream(filePath);

  const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  let count = 0;
  for await (const line of rl) {
    count++;
  }

  console.log(`Total rows (including header): ${count}`);
  console.log(`Data rows (excluding header): ${count - 1}`);
}

// Change path to your CSV file
countCSVRows("./files/users_users.csv");
