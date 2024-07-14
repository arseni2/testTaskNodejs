import axios from "axios";
import { ICharacter, IResponse } from "./types";
import * as fs from "node:fs";
import { Client } from "pg";

const config = {
	connectionString:
		"postgres://candidate:62I8anq3cFq5GYh2u4Lh@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1",
	ssl: {
		rejectUnauthorized: true,
		ca: fs
			.readFileSync("./CA.pem")
			.toString(),
	},
};
const tableName = "ArseniiVoronovRickMortyTable"
const conn = new Client(config);

conn.connect((err) => {
	if (err) throw err;
});

const createTable = async () => {
	await conn.query(`CREATE TABLE IF NOT EXISTS ${tableName} (
        id SERIAL PRIMARY KEY,
        name TEXT,
        data JSONB
    );`);
	console.log("Table created successfully");
};

const fetchCharacters = async (nextPageUrl: string): Promise<IResponse> => {
	try {
		console.log(`START REQUEST URL = ${nextPageUrl}`);
		const response = await axios.get(nextPageUrl);
		return response.data;
	} catch (error) {
		console.error("Error fetching characters:", error);
		throw error;
	}
};

const getAllCharacter = async (): Promise<ICharacter[]> => {
	let characters = [];
	let nextPageUrl = "https://rickandmortyapi.com/api/character?page=1";
	while (nextPageUrl) {
		const response = await fetchCharacters(nextPageUrl);
		nextPageUrl = response.info.next as string;
		characters.push(...response.results);
	}
	return characters;
};

const insertAllCharacters = async (characters: ICharacter[]) => {
	const values = characters
		.map((char) => `('${char.name.replace(/'/g, "''")}', '${JSON.stringify(char).replace(/'/g, "''")}')`)
		.join(",");
	const insertQuery = `
        INSERT INTO ${tableName} (name, data)
        VALUES ${values}
        RETURNING *;
    `;
	try {
		await conn.query(insertQuery);
		console.log("All characters inserted successfully");
	} catch (error) {
		console.error("Ошибка при вставке всех персонажей:", error);
		throw error;
	}
};

createTable()
	.catch((err) => {
		console.log("Не удалось создать таблицу", err);
	})
	.then(() => {
		getAllCharacter()
			.then(async (characters) => {
				await insertAllCharacters(characters);
				console.log("Все персонажи успешно вставлены");
				await conn.end();
			})
			.catch(async (error) => {
				console.error('Ошибка при получении персонажей:', error);
				await conn.end();
			});
	});
