// Cargar variables de entorno
if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config();
}

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const admin = require('firebase-admin');

// Inicializar Firebase Admin (asegúrate de tener el archivo de credenciales)
// Si estás en producción, puedes usar variables de entorno para las credenciales
let serviceAccount;
try {
    // Intentar cargar archivo de credenciales
    serviceAccount = require('./firebase-credentials.json');
} catch (error) {
    // Si no existe, crear desde variables de entorno
    if (process.env.FIREBASE_CREDENTIALS) {
        try {
            serviceAccount = JSON.parse(process.env.FIREBASE_CREDENTIALS);
        } catch (parseError) {
            console.error('Error al parsear credenciales de Firebase:', parseError);
        }
    } else {
        console.error('No se encontró el archivo de credenciales de Firebase ni la variable de entorno');
    }
}

// Inicializar Firebase solo si tenemos credenciales
let db = null;
if (serviceAccount) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: process.env.FIREBASE_DATABASE_URL || "https://juegomemoriademo-default-rtdb.firebaseio.com/"
    });
    db = admin.database();
    console.log('Firebase inicializado correctamente');
} else {
    console.warn('No se pudo inicializar Firebase, se usará solo almacenamiento local');
}

const app = express();

// Rutas para archivos de estado con sistema de respaldo
const GAME_STATE_FILE = path.join(__dirname, 'game-state.json');
const GAME_STATE_BACKUP_1 = path.join(__dirname, 'game-state.backup1.json');
const GAME_STATE_BACKUP_2 = path.join(__dirname, 'game-state.backup2.json');
const ERROR_LOG_FILE = path.join(__dirname, 'error-log.txt');

// Añadir estas nuevas variables para el sistema de mesas
const MAX_TABLES_PER_DAY = 10;
const playerGameState = {}; // Para guardar el estado de juego de cada jugador
const playerTableCount = {}; // Contar mesas jugadas por cada jugador
let globalTableNumber = 1; // Mesa global que todos los jugadores verán

// CAMBIO: Límite de fichas por jugador - ACTUALIZADO A 5
const MAX_SELECTIONS_PER_PLAYER = 5;

// Variables para optimización
const playerInactivityTimeouts = {};
const INACTIVITY_THRESHOLD = 60 * 60 * 1000; // 1 hora

// Variables para control de guardado diferencial en Firebase
let pendingChanges = {};
let saveTimeout = null;

// Configuración de CORS actualizada para permitir múltiples orígenes
const corsOptions = {
    origin: process.env.NODE_ENV === 'production'
        ? [
            process.env.CLIENT_URL || 'https://juegoclientedemo.onrender.com',
            'https://juegoclientedemo.onrender.com' // Sin la barra al final
        ]
        : ['http://localhost:3000'],
    methods: ['GET', 'POST'],
    credentials: true
};

app.use(cors(corsOptions));

const server = http.createServer(app);
const io = new Server(server, {
    cors: corsOptions,
    pingTimeout: 60000,
    pingInterval: 25000,
    connectTimeout: 45000,
    reconnection: true,
    reconnectionAttempts: 10,
    reconnectionDelay: 1000,
    reconnectionDelayMax: 5000
});

// Sistema de ping/heartbeat para verificar conexiones
setInterval(() => {
    // Verificar las conexiones activas
    const onlinePlayers = new Set();

    for (const socketId in connectedSockets) {
        const userId = connectedSockets[socketId];
        const socket = io.sockets.sockets.get(socketId);

        if (socket && socket.connected && userId) {
            onlinePlayers.add(userId);

            // Enviar ping para verificar que está realmente conectado
            socket.emit('ping', {}, (response) => {
                // Este callback solo se ejecutará si el cliente responde
                console.log(`Ping recibido de ${userId}`);
            });
        }
    }

    // Actualizar el estado de conexión en la lista de jugadores
    let connectionChanged = false;

    gameState.players.forEach(player => {
        const wasConnected = player.isConnected;
        player.isConnected = onlinePlayers.has(player.id);

        if (wasConnected !== player.isConnected) {
            connectionChanged = true;
        }
    });

    // Si hubo cambios, notificar a todos los clientes
    if (connectionChanged) {
        io.emit('connectionStatusUpdate', {
            players: gameState.players.map(player => ({
                id: player.id,
                isConnected: player.isConnected
            }))
        });

        // Guardar estado después de cambios de conexión
        saveGameState();
    }
}, 10000); // Verificar cada 10 segundos

// Datos de usuario actualizados para el servidor 3 con nombres diferentes
const users = [
    { id: '1', username: 'Condor', password: 'vuela741', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: '2', username: 'Colibrí', password: 'rapido852', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: '3', username: 'Tucan', password: 'colores963', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: '4', username: 'Quetzal', password: 'sagrado159', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: '5', username: 'Flamingo', password: 'rosado753', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: '6', username: 'Gaviota', password: 'marina426', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: '7', username: 'Pelicano', password: 'pescador817', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: '8', username: 'Canario', password: 'amarillo294', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: '9', username: 'Cisne', password: 'elegante685', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: '10', username: 'Gorrion', password: 'urbano372', score: 60000, prevScore: 60000, isAdmin: false, isBlocked: false, isLockedDueToScore: false },
    { id: 'admin', username: 'admin', password: 'admin1998', score: 60000, prevScore: 60000, isAdmin: true, isBlocked: false, isLockedDueToScore: false }
];

// Mapa de Socket IDs a usuarios
const connectedSockets = {};

// Estado del juego - Persistente incluso cuando no hay jugadores conectados
let gameState = {
    board: generateBoard(),
    players: [],
    currentPlayerIndex: 0,
    currentPlayer: null,
    status: 'playing', // Inicializamos directamente como 'playing' en lugar de 'waiting'
    turnStartTime: null,
    // CAMBIO: Eliminar rowSelections, solo usar totalSelections por jugador
    tableCount: 0,
    lastTableResetDate: new Date().toDateString(),
    playerSelections: {} // Mapa para rastrear selecciones de cada jugador
};

let turnTimer = null;

// Función para encolar cambios para guardado diferencial
function queueGameStateChange(path, value) {
    if (!db) return; // Si no está disponible Firebase, no hacer nada

    pendingChanges[path] = value;

    if (!saveTimeout) {
        saveTimeout = setTimeout(() => {
            const updates = { ...pendingChanges };
            pendingChanges = {};
            saveTimeout = null;

            // Guardar cambios acumulados
            db.ref().update(updates)
                .then(() => console.log('Cambios incrementales guardados en Firebase'))
                .catch(error => console.error('Error al guardar cambios incrementales:', error));
        }, 1000); // Guardar después de 1 segundo de inactividad
    }
}

// Función para validar la integridad del tablero
function validateBoardIntegrity() {
    // Verificar que haya 8 fichas positivas y 8 negativas
    let positiveCount = 0;
    let negativeCount = 0;

    for (const tile of gameState.board) {
        if (tile.value > 0) positiveCount++;
        if (tile.value < 0) negativeCount++;
    }

    if (positiveCount !== 8 || negativeCount !== 8) {
        console.error(`ERROR DE INTEGRIDAD DEL TABLERO: ${positiveCount} positivas, ${negativeCount} negativas`);
        // Regenerar el tablero para corregir
        gameState.board = generateBoard();
        // Guardar estado después de regenerar el tablero
        saveGameState();
        return false;
    }

    return true;
}

// Ejecutar esta validación periódicamente
setInterval(validateBoardIntegrity, 5 * 60 * 1000); // Cada 5 minutos

// Función para verificar si un usuario debe ser bloqueado por puntos exactos
// o por caer a 23,000 o menos
function checkScoreLimit(user) {
    // Sólo bloquear si es jugador y tiene 23,000 puntos o menos
    if (user.score <= 23000 && !user.isAdmin) {
        console.log(`Usuario ${user.username} bloqueado por alcanzar o caer a ${user.score} puntos`);

        // Solo modificar el estado si necesita cambiarse
        if (!user.isLockedDueToScore) {
            user.isLockedDueToScore = true;

            // Notificar inmediatamente al usuario a través de su socket si está conectado
            const playerSocketId = gameState.players.find(p => p.id === user.id)?.socketId;
            if (playerSocketId) {
                io.to(playerSocketId).emit('scoreLimitReached', {
                    message: 'Has alcanzado o caído a 23,000 puntos o menos. Tu cuenta ha sido bloqueada temporalmente.'
                });
                io.to(playerSocketId).emit('blockStatusChanged', {
                    isLockedDueToScore: true,
                    message: 'Has alcanzado o caído a 23,000 puntos o menos. Tu cuenta ha sido bloqueada temporalmente.'
                });
            }

            // Actualizar en Firebase si está disponible
            if (db) {
                queueGameStateChange(`gameState/userScores/${user.id}/isLockedDueToScore`, true);
            }

            // Guardar estado después del bloqueo
            saveGameState();
        }

        return true;
    } else if (user.score > 23000 && user.isLockedDueToScore) {
        // Si el puntaje supera 23,000 pero sigue bloqueado, desbloquearlo
        console.log(`Desbloqueando a ${user.username} porque su puntaje es ${user.score} > 23000`);
        user.isLockedDueToScore = false;

        // Notificar al usuario
        const playerSocketId = gameState.players.find(p => p.id === user.id)?.socketId;
        if (playerSocketId) {
            io.to(playerSocketId).emit('userUnlocked', {
                message: 'Tu puntaje ha superado los 23,000 puntos y tu cuenta ha sido desbloqueada.'
           });
           io.to(playerSocketId).emit('blockStatusChanged', {
               isLockedDueToScore: false,
               message: 'Tu puntaje ha superado los 23,000 puntos y tu cuenta ha sido desbloqueada.'
           });
       }

       // Actualizar en Firebase
       if (db) {
           queueGameStateChange(`gameState/userScores/${user.id}/isLockedDueToScore`, false);
       }

       // Guardar estado después del desbloqueo
       saveGameState();

       return false;
   }

   return user.isLockedDueToScore; // Mantener el estado actual si no hay cambios
}

// Función para verificar la integridad de los estados de bloqueo
function verifyBlockingStates() {
   let inconsistenciasCorregidas = 0;

   // Verificar cada jugador en la lista de usuarios
   for (const user of users) {
       if (user.isAdmin) continue; // Ignorar administradores

       // La única condición válida para bloqueo automático es por puntaje <= 23000
       const shouldBeLockedDueToScore = user.score <= 23000;

       if (user.isLockedDueToScore !== shouldBeLockedDueToScore) {
           console.log(`Corrigiendo inconsistencia de bloqueo por puntaje para ${user.username}: ${user.isLockedDueToScore} -> ${shouldBeLockedDueToScore}`);
           user.isLockedDueToScore = shouldBeLockedDueToScore;
           inconsistenciasCorregidas++;

           // Notificar al usuario si está conectado
           const playerSocketId = gameState.players.find(p => p.id === user.id)?.socketId;
           if (playerSocketId) {
               if (shouldBeLockedDueToScore) {
                   io.to(playerSocketId).emit('scoreLimitReached', {
                       message: 'Has alcanzado o caído a 23,000 puntos o menos. Tu cuenta ha sido bloqueada temporalmente.'
                   });
               } else {
                   io.to(playerSocketId).emit('userUnlocked', {
                       message: 'Tu puntaje ha sido corregido y tu cuenta ha sido desbloqueada.'
                   });
               }

               io.to(playerSocketId).emit('blockStatusChanged', {
                   isLockedDueToScore: shouldBeLockedDueToScore,
                   message: shouldBeLockedDueToScore ?
                       'Tu cuenta ha sido bloqueada por alcanzar o caer a 23,000 puntos o menos.' :
                       'Tu cuenta ha sido desbloqueada.'
               });
           }

           // Actualizar en Firebase
           if (db) {
               queueGameStateChange(`gameState/userScores/${user.id}/isLockedDueToScore`, shouldBeLockedDueToScore);
           }
       }
   }

   if (inconsistenciasCorregidas > 0) {
       console.log(`Se corrigieron ${inconsistenciasCorregidas} inconsistencias de bloqueo por puntaje`);
       saveGameState(); // Guardar cambios
   }

   return inconsistenciasCorregidas;
}

// Verificar la integridad de los estados de bloqueo cada 2 minutos
setInterval(verifyBlockingStates, 2 * 60 * 1000);

// Esta función ahora solo hace monitoreo
function checkAndResetTableCounters() {
   // No realiza ningún reinicio automático, solo monitoreo
   console.log("Verificación de monitoreo - No se realiza reinicio automático");

   // Opcional: Log para verificar la cantidad de mesas por jugador
   for (const userId in playerTableCount) {
       const user = getUserById(userId);
       if (user) {
           console.log(`${user.username}: ${playerTableCount[userId]} mesas jugadas`);
       }
   }
}

// Función para limpiar datos de jugadores inactivos (optimización)
function cleanupInactivePlayersData() {
   const currentTime = Date.now();
   let cleanedCount = 0;

   // Limpiar datos de jugadores que llevan inactivos más de 1 hora
   for (const userId in playerGameState) {
       if (playerGameState[userId].timestamp &&
           (currentTime - playerGameState[userId].timestamp) > INACTIVITY_THRESHOLD) {
           delete playerGameState[userId];
           cleanedCount++;
       }
   }

   if (cleanedCount > 0) {
       console.log(`Limpieza de memoria: ${cleanedCount} jugadores inactivos eliminados`);
   }
}

// Mantener la verificación periódica pero ahora sin reinicio automático
setInterval(checkAndResetTableCounters, 60 * 60 * 1000); // Cada hora
setInterval(cleanupInactivePlayersData, 30 * 60 * 1000); // Cada 30 minutos

// Función para reinicio manual por admin
function adminResetTableCounters() {
   // Reiniciar contadores para todos los jugadores
   Object.keys(playerTableCount).forEach(userId => {
       playerTableCount[userId] = 0;
   });

   gameState.lastTableResetDate = new Date().toDateString();
   gameState.tableCount = 0;

   // Notificar a todos los clientes
   io.emit('tablesUnlocked', { message: 'El administrador ha reiniciado los contadores de mesas.' });

   // Actualizar en Firebase si está disponible
   if (db) {
       queueGameStateChange('gameState/lastTableResetDate', gameState.lastTableResetDate);
       queueGameStateChange('gameState/tableCount', 0);

       const updates = {};
       Object.keys(playerTableCount).forEach(userId => {
           updates[`gameState/userScores/${userId}/tablesPlayed`] = 0;
       });

       if (Object.keys(updates).length > 0) {
           db.ref().update(updates).catch(error =>
               console.error('Error al reiniciar contadores de mesa en Firebase:', error)
           );
       }
   }

   console.log('Contadores de mesas reiniciados por administrador');
   saveGameState();
}

// Modificar la verificación de mesas
// Si es mesa 5 y el jugador ha completado exactamente el máximo, permitir jugar
// para que pueda completar el ciclo
function checkTableLimit(userId) {
   if (!playerTableCount[userId]) {
       playerTableCount[userId] = 0;
   }

   // Si es mesa 10 y el jugador ha completado exactamente el máximo, permitir jugar
   // para que pueda completar el ciclo
   if (globalTableNumber === 10 && playerTableCount[userId] === MAX_TABLES_PER_DAY) {
       return false; // No bloquear en este caso especial
   }

   return playerTableCount[userId] > MAX_TABLES_PER_DAY;
}

// Función para incrementar el contador de mesas
function incrementTableCount(userId) {
   if (!playerTableCount[userId]) {
       playerTableCount[userId] = 0;
   }

   playerTableCount[userId]++;
   gameState.tableCount++;

   // Actualizar en Firebase si está disponible
   if (db) {
       queueGameStateChange(`gameState/userScores/${userId}/tablesPlayed`, playerTableCount[userId]);
       queueGameStateChange('gameState/tableCount', gameState.tableCount);
   }

   // Guardar estado
   saveGameState();

   return playerTableCount[userId];
}

// Función mejorada para guardar el estado del juego con Firebase y respaldos locales
async function saveGameState() {
   const stateToSave = {
       board: gameState.board,
       players: gameState.players.map(player => ({
           id: player.id,
           username: player.username,
           socketId: player.socketId || null, // Usar null en lugar de undefined
           isConnected: player.isConnected || false // Asegurar valor booleano válido
       })),
       currentPlayerIndex: gameState.currentPlayerIndex || 0,
       status: gameState.status || 'playing',
       playerSelections: gameState.playerSelections || {},
       tableCount: gameState.tableCount || 0,
       lastTableResetDate: gameState.lastTableResetDate || new Date().toDateString(),
       globalTableNumber: globalTableNumber || 1,
       userScores: users.reduce((obj, user) => {
           obj[user.id] = {
               score: user.score || 60000,
               prevScore: user.prevScore || 60000,
               isBlocked: user.isBlocked || false,
               isLockedDueToScore: user.isLockedDueToScore || false,
               tablesPlayed: playerTableCount[user.id] || 0,
               username: user.username, // IMPORTANTE: Incluir el username actualizado
               password: user.password  // IMPORTANTE: Incluir la contraseña actualizada
           };
           return obj;
       }, {}),
       playerGameStates: playerGameState || {},
       timestamp: Date.now(), // Añadir timestamp para verificación
       // NUEVO: Guardar el array completo de usuarios modificados
       modifiedUsers: users.map(user => ({
           id: user.id,
           username: user.username,
           password: user.password,
           score: user.score,
           prevScore: user.prevScore,
           isAdmin: user.isAdmin,
           isBlocked: user.isBlocked,
           isLockedDueToScore: user.isLockedDueToScore
       }))
   };

   const jsonData = JSON.stringify(stateToSave, null, 2);
   let savedSuccessfully = false;

   // Intentar guardar en Firebase primero si está disponible
   if (db) {
       try {
           await db.ref('gameState').set(stateToSave);
           console.log('Estado del juego guardado correctamente en Firebase');
           savedSuccessfully = true;
       } catch (firebaseError) {
           console.error('Error al guardar en Firebase, intentando respaldo local:', firebaseError);
       }
   }

   // Siempre guardar en local como respaldo, incluso si Firebase tuvo éxito
   try {
       // Sistema de rotación de respaldos
       // 1. Si existe el archivo principal, copiarlo como backup1
       if (fs.existsSync(GAME_STATE_FILE)) {
           try {
               const mainFileContent = fs.readFileSync(GAME_STATE_FILE, 'utf8');
               fs.writeFileSync(GAME_STATE_BACKUP_1, mainFileContent);
           } catch (backupError) {
               console.error('Error al crear respaldo 1:', backupError);
           }
       }

       // 2. Si existe backup1, copiarlo como backup2
       if (fs.existsSync(GAME_STATE_BACKUP_1)) {
           try {
               const backup1Content = fs.readFileSync(GAME_STATE_BACKUP_1, 'utf8');
               fs.writeFileSync(GAME_STATE_BACKUP_2, backup1Content);
           } catch (backupError) {
               console.error('Error al crear respaldo 2:', backupError);
           }
       }

       // 3. Guardar el nuevo estado en el archivo principal
       fs.writeFileSync(GAME_STATE_FILE, jsonData);
       console.log('Estado del juego guardado correctamente en archivos locales');
       savedSuccessfully = true;
   } catch (error) {
       console.error('Error al guardar el estado del juego en archivos locales:', error);

       if (!savedSuccessfully) {
           try {
               // Intentar guardar directamente en los archivos de respaldo
               fs.writeFileSync(GAME_STATE_BACKUP_1, jsonData);
               console.log('Estado guardado en respaldo 1 tras error en archivo principal');
               savedSuccessfully = true;
           } catch (backup1Error) {
               console.error('Error al guardar en respaldo 1:', backup1Error);
               try {
                   fs.writeFileSync(GAME_STATE_BACKUP_2, jsonData);
                   console.log('Estado guardado en respaldo 2 tras errores previos');
                   savedSuccessfully = true;
               } catch (backup2Error) {
                   console.error('Error crítico: No se pudo guardar el estado en ninguna ubicación');
               }
           }
       }

       try {
           fs.appendFileSync(ERROR_LOG_FILE, `${new Date().toISOString()} - Error guardando estado: ${error.message}\n`);
       } catch (logError) {
           console.error('Error adicional al escribir en archivo de log');
       }
   }

   return savedSuccessfully;
}

// Función mejorada para cargar el estado con Firebase y múltiples respaldos
async function loadGameState() {
   let loadedState = null;
   let loadedSource = null;

   // 1. Intentar cargar desde Firebase primero
   if (db) {
       try {
           console.log('Intentando cargar estado desde Firebase...');
           const snapshot = await db.ref('gameState').once('value');
           const firebaseState = snapshot.val();

           if (firebaseState &&
               firebaseState.board &&
               Array.isArray(firebaseState.board) &&
               firebaseState.board.length === 16) {
               loadedState = firebaseState;
               loadedSource = 'Firebase';
               console.log('Estado cargado exitosamente desde Firebase');
           } else {
               console.warn('Firebase contiene datos pero estructura inválida o incompleta');
           }
       } catch (firebaseError) {
           console.error('Error al cargar desde Firebase:', firebaseError);
       }
   }

   // 2. Si no se pudo cargar desde Firebase, intentar desde archivos locales
   if (!loadedState) {
       const fileOptions = [GAME_STATE_FILE, GAME_STATE_BACKUP_1, GAME_STATE_BACKUP_2];

       // Intentar cargar desde cada archivo en orden
       for (const file of fileOptions) {
           try {
               if (fs.existsSync(file)) {
                   const fileContent = fs.readFileSync(file, 'utf8');
                   if (fileContent && fileContent.trim() !== '') {
                       const parsedState = JSON.parse(fileContent);

                       // Verificar que el estado tenga la estructura mínima necesaria
                       if (parsedState &&
                           parsedState.board &&
                           Array.isArray(parsedState.board) &&
                           parsedState.board.length === 16) {
                           loadedState = parsedState;
                           loadedSource = file;
                           console.log(`Estado cargado exitosamente desde: ${file}`);
                           break; // Salir del bucle si se cargó correctamente
                       } else {
                           console.warn(`Archivo ${file} existe pero tiene estructura inválida`);
                       }
                   } else {
                       console.warn(`Archivo ${file} está vacío`);
                   }
               }
           } catch (error) {
               console.error(`Error al cargar desde ${file}:`, error);
               try {
                   fs.appendFileSync(ERROR_LOG_FILE, `${new Date().toISOString()} - Error cargando desde ${file}: ${error.message}\n`);
               } catch (logError) { }
           }
       }
   }

   // 3. Aplicar el estado cargado si existe
   if (loadedState) {
       // NUEVO: Cargar usuarios modificados si existen
       if (loadedState.modifiedUsers && Array.isArray(loadedState.modifiedUsers)) {
           console.log('Cargando usuarios modificados desde el estado guardado...');

           // Actualizar el array de usuarios con los datos guardados
           loadedState.modifiedUsers.forEach(savedUser => {
               const userIndex = users.findIndex(u => u.id === savedUser.id);
               if (userIndex !== -1) {
                   // Actualizar solo los campos que pueden cambiar
                   users[userIndex].username = savedUser.username;
                   users[userIndex].password = savedUser.password;
                   users[userIndex].score = savedUser.score;
                   users[userIndex].prevScore = savedUser.prevScore;
                   users[userIndex].isBlocked = savedUser.isBlocked;
                   users[userIndex].isLockedDueToScore = savedUser.isLockedDueToScore;

                   console.log(`Usuario ${savedUser.id} actualizado: username=${savedUser.username}`);
               }
           });
       }

       // Restaurar el estado del juego completo
       if (loadedState.board) {
           gameState.board = loadedState.board;
       }

       if (loadedState.tableCount !== undefined) {
           gameState.tableCount = loadedState.tableCount;
       }

       if (loadedState.lastTableResetDate) {
           gameState.lastTableResetDate = loadedState.lastTableResetDate;
       }

       if (loadedState.globalTableNumber !== undefined) {
           globalTableNumber = loadedState.globalTableNumber;
       }

       if (loadedState.playerSelections) {
           gameState.playerSelections = loadedState.playerSelections;
       }

       if (loadedState.playerGameStates) {
           Object.assign(playerGameState, loadedState.playerGameStates);
       }

       // Cargar puntuaciones y estados de usuario
       if (loadedState.userScores) {
           for (const userId in loadedState.userScores) {
               const user = users.find(u => u.id === userId);
               if (user) {
                   user.score = loadedState.userScores[userId].score;
                   user.prevScore = loadedState.userScores[userId].prevScore || user.score;
                   user.isBlocked = loadedState.userScores[userId].isBlocked;
                   user.isLockedDueToScore = loadedState.userScores[userId].isLockedDueToScore || false;

                   // IMPORTANTE: Cargar también username y password actualizados
                   if (loadedState.userScores[userId].username) {
                       user.username = loadedState.userScores[userId].username;
                   }
                   if (loadedState.userScores[userId].password) {
                       user.password = loadedState.userScores[userId].password;
                   }

                   if (loadedState.userScores[userId].tablesPlayed !== undefined) {
                       playerTableCount[userId] = loadedState.userScores[userId].tablesPlayed;
                  }
              }
          }
      }

      if (loadedState.players) {
          gameState.players = loadedState.players.map(player => ({
              ...player,
              isConnected: false
          }));
      }

      // 4. Si se cargó desde archivo local pero tenemos Firebase, sincronizar con Firebase
      if (loadedSource !== 'Firebase' && db) {
          console.log('Sincronizando estado cargado con Firebase...');
          try {
              await db.ref('gameState').set(loadedState);
              console.log('Estado sincronizado correctamente con Firebase');
          } catch (syncError) {
              console.error('Error al sincronizar con Firebase:', syncError);
          }
      }

      // 5. Verificar la integridad del tablero
      validateBoardIntegrity();

      console.log(`Estado del juego cargado correctamente desde ${loadedSource}. Mesa global actual: ${globalTableNumber}`);
      return true;
  }

  // Si no se pudo cargar ningún estado, inicializar con valores predeterminados
  console.warn('NO SE PUDO CARGAR NINGÚN ESTADO VÁLIDO - INICIALIZANDO CON VALORES PREDETERMINADOS');
  gameState.board = generateBoard();
  globalTableNumber = 1;

  // Si tenemos Firebase disponible, guardar el estado inicial
  if (db) {
      try {
          const initialState = {
              board: gameState.board,
              globalTableNumber: 1,
              tableCount: 0,
              lastTableResetDate: new Date().toDateString(),
              status: 'playing',
              timestamp: Date.now()
          };
          await db.ref('gameState').set(initialState);
          console.log('Estado inicial guardado en Firebase');
      } catch (initError) {
          console.error('Error al guardar estado inicial en Firebase:', initError);
      }
  }

  return false;
}

// Intentar cargar el estado guardado
(async function () {
  try {
      if (!await loadGameState()) {
          console.log('No se encontró estado guardado o hubo un error al cargarlo, usando valores predeterminados');
      }
  } catch (err) {
      console.error('Error durante la carga inicial del estado:', err);
  }
})();

// Generar el tablero con distribución aleatoria de fichas ganadoras y perdedoras en cada hilera
function generateBoard() {
  const tiles = [];

  // Para cada hilera (4 hileras en total, con 4 fichas cada una)
  for (let row = 0; row < 4; row++) {
      const rowTiles = [];

      // Crear 2 fichas ganadoras y 2 perdedoras para esta hilera
      for (let i = 0; i < 2; i++) {
          rowTiles.push({ value: 15000, revealed: false });  
      }
      for (let i = 0; i < 2; i++) {
          rowTiles.push({ value: -16000, revealed: false }); 
      }

      // Mezclar las fichas dentro de esta hilera
      const shuffledRowTiles = shuffleArray(rowTiles);

      // Añadir las fichas mezcladas de esta hilera al tablero
      tiles.push(...shuffledRowTiles);
  }

  // Log para verificar la distribución
  let gainTiles = 0;
  let lossTiles = 0;
  const distribution = [0, 0, 0, 0]; // Contar fichas ganadoras por fila

  for (let i = 0; i < tiles.length; i++) {
      if (tiles[i].value > 0) {
          gainTiles++;
          distribution[Math.floor(i / 4)]++;
      } else {
          lossTiles++;
      }
  }

  console.log(`Distribución de tablero: ${gainTiles} ganadoras (+15000), ${lossTiles} perdedoras (-16000)`);
  console.log(`Fichas ganadoras por fila: Fila 1: ${distribution[0]}, Fila 2: ${distribution[1]}, Fila 3: ${distribution[2]}, Fila 4: ${distribution[3]}`);

  return tiles;
}

// Función para mezclar un array (algoritmo Fisher-Yates)
function shuffleArray(array) {
  const newArray = [...array];
  for (let i = newArray.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [newArray[i], newArray[j]] = [newArray[j], newArray[i]];
  }
  return newArray;
}

// CAMBIO: Función para verificar si el juego terminó (basada en fichas reveladas del tablero)
function checkGameOver() {
   // El juego termina cuando todas las fichas del tablero están reveladas
   let revealedCount = 0;
   for (const tile of gameState.board) {
       if (tile.revealed) {
           revealedCount++;
       }
   }
   // Si todas las 16 fichas están reveladas, el juego terminó
   return revealedCount >= 16;
}

// Obtener usuario por ID
function getUserById(id) {
  return users.find(user => user.id === id);
}

// Actualizar la puntuación de un usuario
function updateUserScore(id, points) {
  const user = getUserById(id);
  if (user) {
      console.log(`Actualizando puntuación de ${user.username}: ${user.score} + ${points}`);
      // Guardar puntuación anterior
      user.prevScore = user.score;
      user.score += points;
      console.log(`Nueva puntuación: ${user.score}`);

      // Verificar si debe ser bloqueado cuando llega a 23000 o menos
      checkScoreLimit(user);

      // Actualizar en Firebase si está disponible
      if (db) {
          queueGameStateChange(`gameState/userScores/${id}/score`, user.score);
          queueGameStateChange(`gameState/userScores/${id}/prevScore`, user.prevScore);
      }

      // Guardar el estado después de actualizar la puntuación
      saveGameState();

      return user.score;
  }
  console.error(`Usuario con ID ${id} no encontrado para actualizar puntuación`);
  return null;
}

// CAMBIO: Función para inicializar selecciones de un jugador (nueva estructura con persistencia)
function initPlayerSelections(userId) {
  if (!gameState.playerSelections[userId]) {
      gameState.playerSelections[userId] = {
          totalSelected: 0,
          showPermanentModal: false,
          hasCompletedSelections: false // NUEVO: para persistir el estado de completado
      };

      // Actualizar en Firebase si está disponible
      if (db) {
          queueGameStateChange(`gameState/playerSelections/${userId}`, gameState.playerSelections[userId]);
      }
  }
  return gameState.playerSelections[userId];
}

// Función para verificar y corregir problemas conocidos
function verifyAndFixGameState() {
  console.log("Verificando integridad del estado del juego...");

  // 1. Verificar que no haya fichas en estado inconsistente
  let fichasCorregidas = 0;
  for (let i = 0; i < gameState.board.length; i++) {
      // Si la ficha no existe o tiene valores inválidos, corregirla
      if (!gameState.board[i] || gameState.board[i].value === undefined) {
          gameState.board[i] = {
              value: (Math.random() > 0.5 ? 15000 : -16000), // Cambiar valores
              revealed: false
          };
          fichasCorregidas++;
      }
  }

  // 2. Verificar que el tablero tenga el balance correcto (8 positivas, 8 negativas)
  validateBoardIntegrity();

  // 3. Verificar que los jugadores no tengan estados de bloqueo inconsistentes
  const inconsistenciasCorregidas = verifyBlockingStates();

  // 4. Verificar que el contador de mesas esté en rango válido
  if (globalTableNumber < 1 || globalTableNumber > 10) {
      console.log(`Corrigiendo número de mesa inválido: ${globalTableNumber}`);
      globalTableNumber = 1;
  }

  // 5. Si detectamos alguna corrección, guardar el estado corregido
  if (fichasCorregidas > 0 || inconsistenciasCorregidas > 0) {
      console.log(`Se realizaron correcciones: ${fichasCorregidas} fichas, ${inconsistenciasCorregidas} estados de bloqueo`);
      saveGameState();
  }
}

// Ejecutar cada 5 minutos para mantener el juego en buen estado
setInterval(verifyAndFixGameState, 5 * 60 * 1000);

// Función para reiniciar solo el tablero y asegurar el orden de mesas
async function resetBoardOnly() {
  console.log("Reiniciando el tablero y avanzando a la siguiente mesa");

  // Incrementar el número de mesa global de manera ordenada
  globalTableNumber++;
  if (globalTableNumber > 10) {
      globalTableNumber = 1; // Volver a la mesa 1 después de la 10
      console.log("Ciclo completado de 10 mesas, volviendo a la mesa 1");
  }

  // Crear nuevo tablero sin fichas reveladas
  gameState.board = generateBoard();

  // CAMBIO: NO reiniciar selecciones de jugadores que ya completaron sus 5 fichas
  for (const userId in gameState.playerSelections) {
      // Solo reiniciar si el jugador NO ha completado sus selecciones
      if (!gameState.playerSelections[userId].hasCompletedSelections) {
          gameState.playerSelections[userId].totalSelected = 0;
          gameState.playerSelections[userId].showPermanentModal = false;
      }
      // Si ya completó, mantener su estado bloqueado
  }

  // IMPORTANTE: Garantizar que no se modifique el estado de bloqueo
  // Verificar qué jugadores están realmente conectados sin modificar bloqueos
  const connectedPlayerIds = new Set();
  Object.keys(connectedSockets).forEach(socketId => {
      const userId = connectedSockets[socketId];
      if (userId) {
          connectedPlayerIds.add(userId);
      }
  });

  // Actualizar SOLO el estado de conexión de los jugadores, no modificar bloqueos
  for (const player of gameState.players) {
      // Solo actualizamos el estado de conexión, no modificamos el estado de bloqueo
      player.isConnected = connectedPlayerIds.has(player.id);

      // NO modificar estados de bloqueo aquí
  }

  // Si solo hay un jugador conectado, establecerlo como el jugador actual
  // Verificar que no sea admin y que no esté bloqueado
  const eligiblePlayers = gameState.players.filter(player => {
      const userData = getUserById(player.id);
      return player.isConnected && userData && !userData.isBlocked && !userData.isLockedDueToScore && !userData.isAdmin;
  });

  if (eligiblePlayers.length === 1) {
      gameState.currentPlayer = eligiblePlayers[0];
      gameState.currentPlayerIndex = gameState.players.findIndex(p => p.id === eligiblePlayers[0].id);
  }

  // Actualizar toda la información crítica en Firebase inmediatamente si está disponible
  if (db) {
      try {
          const criticalUpdates = {
              'gameState/board': gameState.board,
              'gameState/globalTableNumber': globalTableNumber,
              'gameState/playerSelections': gameState.playerSelections,
              'gameState/players': gameState.players.map(p => ({
                  id: p.id,
                  username: p.username,
                  socketId: p.socketId,
                  isConnected: p.isConnected
                  // NO incluir estados de bloqueo aquí
              })),
              'gameState/currentPlayerIndex': gameState.currentPlayerIndex
          };

          await db.ref().update(criticalUpdates);
          console.log('Información de nuevo tablero actualizada en Firebase');
      } catch (firebaseError) {
          console.error('Error al actualizar nuevo tablero en Firebase:', firebaseError);
      }
  }

  // Notificar a todos los clientes del cambio de mesa con tablero nuevo
  // Asegurarse de no enviar información que pueda afectar estados de bloqueo
  io.emit('boardReset', {
      message: "Todas las fichas fueron reveladas. ¡Avanzando a la mesa " + globalTableNumber + "!",
      newTableNumber: globalTableNumber,
      newBoard: gameState.board, // Enviar el tablero nuevo completo
      connectedPlayers: gameState.players.filter(p => p.isConnected).map(p => p.id)
  });

  // Actualizar estado de contadores de mesa para cada jugador
  for (const player of gameState.players) {
      const playerId = player.id;
      const playerUser = getUserById(playerId);

      // Verificar que el usuario exista y no esté bloqueado por puntaje
      if (!playerUser || playerUser.isLockedDueToScore) continue;

      if (!playerTableCount[playerId]) {
          playerTableCount[playerId] = 0;
      }

      playerTableCount[playerId]++;

      // Actualizar contador en Firebase
      if (db) {
          queueGameStateChange(`gameState/userScores/${playerId}/tablesPlayed`, playerTableCount[playerId]);
      }

      // Enviar actualización del contador de mesas
      const playerSocketId = player.socketId;
      if (playerSocketId && player.isConnected) {
          io.to(playerSocketId).emit('tablesUpdate', {
              tablesPlayed: playerTableCount[playerId],
              currentTable: globalTableNumber,
              maxReached: playerTableCount[playerId] >= MAX_TABLES_PER_DAY,
              lockReason: playerTableCount[playerId] >= MAX_TABLES_PER_DAY ?
                  'Has alcanzado el límite diario de mesas.' : ''
          });

          // Si alcanzó el límite, notificar
          if (playerTableCount[playerId] >= MAX_TABLES_PER_DAY) {
              io.to(playerSocketId).emit('tableLimitReached', {
                  message: 'Has alcanzado el límite diario de mesas.'
              });
          }
      }
  }

  // Emitir nuevo estado del juego sin modificar estados de bloqueo
  io.emit('gameState', {
      board: gameState.board,
      currentPlayer: gameState.currentPlayer,
      players: gameState.players.map(player => {
          const userData = getUserById(player.id);
          return {
              id: player.id,
              username: player.username,
              isBlocked: userData ? userData.isBlocked : false, // Usar el valor real de la lista de usuarios
              isLockedDueToScore: userData ? userData.isLockedDueToScore : false, // Usar el valor real
              isConnected: player.isConnected
          };
      }),
      status: 'playing'
  });

  // Ejecutar verificación adicional para corregir posibles problemas
  verifyAndFixGameState();

  // Guardar estado actualizado
  await saveGameState();
}

async function resetGame() {
  // Crear un nuevo tablero
  const newBoard = generateBoard();

  // Inicializar tablero
  gameState.board = newBoard;
  gameState.status = 'playing';
  gameState.currentPlayerIndex = 0;
  gameState.turnStartTime = Date.now();

  // CAMBIO: Reiniciar selecciones para el nuevo sistema
  gameState.playerSelections = {}; // Limpiar completamente todas las selecciones

  // Reinicializar las selecciones vacías para cada jugador conectado
  gameState.players.forEach(player => {
      if (player.id) {
          gameState.playerSelections[player.id] = {
              totalSelected: 0,
              showPermanentModal: false,
              hasCompletedSelections: false // NUEVO: reiniciar también el estado de completado
          };
      }
  });

  // Reiniciar el número de mesa global
  globalTableNumber = 1;

  // Reiniciar el puntaje de todos los jugadores a 60,000
  users.forEach(user => {
      if (!user.isAdmin) {
          user.prevScore = 60000;
          user.score = 60000;
          user.isBlocked = false;
          user.isLockedDueToScore = false; // Desbloquear por puntaje también
      }
  });

  // Reiniciar contadores de mesas
  for (const userId in playerTableCount) {
      playerTableCount[userId] = 0;
  }

  // IMPORTANTE: Verificar qué jugadores están realmente conectados
  const reallyConnectedPlayers = new Set();

  // Recorrer las conexiones activas para determinar qué jugadores están realmente conectados
  for (const socketId in connectedSockets) {
      const userId = connectedSockets[socketId];
      if (userId) {
          // Verificar que el socket esté realmente conectado
          const socket = io.sockets.sockets.get(socketId);
          if (socket && socket.connected) {
              reallyConnectedPlayers.add(userId);
          }
      }
  }

  console.log("Jugadores realmente conectados:", Array.from(reallyConnectedPlayers));

  // Actualizar el estado de conexión en la lista de jugadores
  gameState.players.forEach(player => {
      player.isConnected = reallyConnectedPlayers.has(player.id);

      // IMPORTANTE: Garantizar que cada jugador en la lista realmente esté desbloqueado
      const userObject = getUserById(player.id);
      if (userObject && !userObject.isAdmin) {
          userObject.isBlocked = false;
          userObject.isLockedDueToScore = false;
      }

      // Actualizar el socketId si es necesario
      if (!player.isConnected) {
          player.socketId = null;
      }
  });

  // Seleccionar jugador conectado como jugador actual SÓLO si no es admin
  const eligiblePlayers = gameState.players.filter(player => {
      const userData = getUserById(player.id);
      return player.isConnected && userData && !userData.isAdmin;
  });

  if (eligiblePlayers.length > 0) {
      gameState.currentPlayer = eligiblePlayers[0];
      gameState.currentPlayerIndex = gameState.players.findIndex(p => p.id === eligiblePlayers[0].id);
  } else {
      gameState.currentPlayer = null;
      gameState.currentPlayerIndex = 0;
  }

  clearTimeout(turnTimer);

  // Notificar a todos los clientes con un evento explícito para el estado de conexión
  io.emit('connectionStatusUpdate', {
      players: gameState.players.map(player => ({
          id: player.id,
          isConnected: player.isConnected
      }))
  });

  // Actualizar en Firebase si está disponible
  if (db) {
      try {
          // Crear un objeto con todos los updates necesarios
          const resetUpdates = {
              'gameState/board': gameState.board,
              'gameState/status': 'resetCompleted', // Marcar específicamente como resetCompleted
              'gameState/globalTableNumber': 1,
              'gameState/playerSelections': gameState.playerSelections, // Incluir todas las selecciones reiniciadas
              'gameState/turnStartTime': Date.now()
          };

          // Añadir reset de puntajes de todos los usuarios
          users.forEach(user => {
              if (!user.isAdmin) {
                  resetUpdates[`gameState/userScores/${user.id}/score`] = 60000;
                  resetUpdates[`gameState/userScores/${user.id}/prevScore`] = 60000;
                  resetUpdates[`gameState/userScores/${user.id}/isBlocked`] = false;
                  resetUpdates[`gameState/userScores/${user.id}/isLockedDueToScore`] = false;
                  resetUpdates[`gameState/userScores/${user.id}/tablesPlayed`] = 0;
              }
          });

          // Enviar todos los updates de una vez
          await db.ref().update(resetUpdates);
          console.log('Reinicio de juego actualizado en Firebase');
      } catch (firebaseError) {
          console.error('Error al actualizar reinicio en Firebase:', firebaseError);
      }
  }

  // Notificar el estado de resetCompleted a todos los clientes
  io.emit('gameState', {
      board: gameState.board.map(tile => ({
          ...tile,
          value: tile.revealed ? tile.value : null
      })),
      currentPlayer: gameState.currentPlayer,
      players: gameState.players.map(player => ({
          id: player.id,
          username: player.username,
          isBlocked: getUserById(player.id).isBlocked,
          isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
          isConnected: player.isConnected
      })),
      status: 'resetCompleted', // Usar este estado específico para que los clientes sepan que es un reinicio
      turnStartTime: gameState.turnStartTime,
      playerSelections: gameState.playerSelections // Incluir selecciones reiniciadas por jugador
  });

  // Enviar evento específico para reinicio completo con conexión verificada
  io.emit('gameCompletelyReset', {
      message: "El juego ha sido reiniciado completamente",
      newBoard: gameState.board,
      status: 'playing',
      players: gameState.players.map(player => ({
          id: player.id,
          username: player.username,
          isConnected: player.isConnected // Estado de conexión verificado
      })),
      playerSelections: gameState.playerSelections // Incluir las selecciones reiniciadas
  });

  // Enviar puntajes actualizados a todos los jugadores
  gameState.players.forEach(player => {
      const user = getUserById(player.id);
      if (user && player.socketId) {
          io.to(player.socketId).emit('forceScoreUpdate', user.score);
          // Notificar el cambio de estado de bloqueo
          io.to(player.socketId).emit('blockStatusChanged', {
              isLockedDueToScore: false,
              isBlocked: false,
              message: 'El administrador ha reiniciado el juego. Tu puntaje ha sido restablecido a 60,000.'
          });

          // Enviar actualización de mesas
          io.to(player.socketId).emit('tablesUpdate', {
              tablesPlayed: 0,
              currentTable: 1,
              maxReached: false,
              lockReason: ''
          });
      }
  });

  // Notificar a todos los jugadores del reinicio
  io.emit('boardReset', {
      message: "El administrador ha reiniciado el juego. Todos los puntajes han sido restablecidos a 60,000.",
      newTableNumber: 1,
      newBoard: gameState.board
  });

  // Enviar mensaje específico sobre el reinicio de las selecciones
  io.emit('gameResetMessage', {
      message: "Todas las selecciones han sido reiniciadas.",
      command: "resetComplete"
  });

  // Cambiar el estado a 'playing' después de un pequeño retraso para dar tiempo a los clientes a procesar
  setTimeout(() => {
      gameState.status = 'playing';
      if (gameState.players.length > 0) {
          startPlayerTurn();
      }
      // Notificar que ahora estamos en modo de juego
      io.emit('gameState', {
          status: 'playing',
          currentPlayer: gameState.currentPlayer
      });

      // NUEVO: Forzar actualización de estado para todos
      io.emit('forceGameStateRefresh', {
          board: gameState.board,
          currentPlayer: gameState.currentPlayer,
          players: gameState.players.map(player => ({
              id: player.id,
              username: player.username,
              isBlocked: getUserById(player.id).isBlocked,
              isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
              isConnected: player.isConnected
          })),
          status: 'playing',
          playerSelections: gameState.playerSelections, // Incluir todas las selecciones reiniciadas
          canSelectTiles: true // Asegurar que los jugadores puedan seleccionar nuevamente
      });
  }, 2000);

  // Guardar estado después del reset
  await saveGameState();
}

// Función para sincronizar el estado del jugador - mejorada para consistencia
function syncPlayerState(userId, socketId) {
  const user = getUserById(userId);
  if (!user) return;

  // Enviar puntaje actualizado
  io.to(socketId).emit('forceScoreUpdate', user.score);

  // Enviar información de las mesas
  io.to(socketId).emit('tablesUpdate', {
      tablesPlayed: playerTableCount[userId] || 0,
      currentTable: globalTableNumber,
      maxReached: (playerTableCount[userId] || 0) >= MAX_TABLES_PER_DAY,
      lockReason: (playerTableCount[userId] || 0) >= MAX_TABLES_PER_DAY ?
          'Has alcanzado el límite diario de mesas.' : ''
  });

  // NUEVO: Verificar si el jugador debe mostrar el modal permanente
  if (gameState.playerSelections[userId] && gameState.playerSelections[userId].hasCompletedSelections) {
      io.to(socketId).emit('showPermanentModal', {
          playerId: userId,
          message: 'para jugar en nuestro juego real escríbenos en WhatsApp y te brindaremos toda la información'
      });
  }

  // Enviar estado completo del juego
  io.to(socketId).emit('gameState', {
      board: gameState.board.map(tile => ({
          ...tile,
          value: tile.revealed ? tile.value : null
      })),
      currentPlayer: gameState.currentPlayer,
      players: gameState.players.map(player => ({
          id: player.id,
          username: player.username,
          isBlocked: getUserById(player.id).isBlocked,
          isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
          isConnected: player.isConnected
      })),
      status: 'playing',
      totalSelections: gameState.playerSelections[userId]?.totalSelected || 0
  });
}

// Función para iniciar el turno de un jugador, optimizada para evitar problemas
function startPlayerTurn() {
  if (gameState.players.length === 0) return;

  console.log(`startPlayerTurn llamada con ${gameState.players.length} jugadores`);
  gameState.status = 'playing';

  // MODIFICADO: Filtrar solo jugadores conectados y no bloqueados
  // Usar directamente la información de la lista de usuarios para mayor precisión
  let eligiblePlayers = gameState.players.filter(player => {
      const userData = getUserById(player.id);
      // Verificar que el jugador esté conectado, no bloqueado y no sea admin
      return player.isConnected &&
          userData &&
          !userData.isBlocked &&
          !userData.isLockedDueToScore &&
          !userData.isAdmin &&
          (!gameState.playerSelections[player.id] || !gameState.playerSelections[player.id].hasCompletedSelections); // NUEVO: No incluir jugadores que ya completaron
  });

  if (eligiblePlayers.length === 0) {
      console.log("No hay jugadores elegibles, esperando reconexión o desbloqueo...");
      return;
  }

  if (eligiblePlayers.length === 1) {
      // Encontrar el índice del jugador elegible en la lista principal
      const eligiblePlayerIndex = gameState.players.findIndex(p => p.id === eligiblePlayers[0].id);
      gameState.currentPlayerIndex = eligiblePlayerIndex;
      gameState.currentPlayer = gameState.players[eligiblePlayerIndex];

      // IMPORTANTE: Si solo hay un jugador, hacerlo siempre el jugador actual
      clearTimeout(turnTimer);
      turnTimer = setTimeout(() => {
          console.log(`Tiempo agotado para ${gameState.currentPlayer.username}`);
          io.emit('turnTimeout', { playerId: gameState.currentPlayer.id });
          setTimeout(() => {
              startPlayerTurn();
          }, 500);
      }, 6000); // Cambiado a 6 segundos para coincidir con el frontend
  } else {
      // Para múltiples jugadores, buscar el siguiente jugador elegible
      let nextPlayerFound = false;
      let loopCount = 0;
      let originalIndex = gameState.currentPlayerIndex;

      // Comenzar desde el siguiente jugador
      gameState.currentPlayerIndex = (gameState.currentPlayerIndex + 1) % gameState.players.length;

      while (!nextPlayerFound && loopCount < gameState.players.length) {
          const nextPlayer = gameState.players[gameState.currentPlayerIndex];
          const nextUserData = getUserById(nextPlayer.id);

          // Solo considerar jugadores conectados, no bloqueados, que no sean admin y que no hayan completado
          if (nextPlayer.isConnected && nextUserData &&
              !nextUserData.isBlocked && !nextUserData.isLockedDueToScore && !nextUserData.isAdmin &&
              (!gameState.playerSelections[nextPlayer.id] || !gameState.playerSelections[nextPlayer.id].hasCompletedSelections)) {
              nextPlayerFound = true;
          } else {
              gameState.currentPlayerIndex = (gameState.currentPlayerIndex + 1) % gameState.players.length;
              loopCount++;
          }
      }

      // Si no encontramos un jugador válido, mantener el índice original
      if (!nextPlayerFound) {
          console.log("No hay jugadores elegibles para el siguiente turno");
          gameState.currentPlayerIndex = originalIndex;
          // Intentar nuevamente en unos segundos
          setTimeout(() => {
              startPlayerTurn();
          }, 5000);
          return;
      }

      gameState.currentPlayer = gameState.players[gameState.currentPlayerIndex];
      console.log(`Turno de ${gameState.currentPlayer.username}, tiene 6 segundos`);

      clearTimeout(turnTimer);
      turnTimer = setTimeout(() => {
          console.log(`Tiempo agotado para ${gameState.currentPlayer.username}`);
          io.emit('turnTimeout', { playerId: gameState.currentPlayer.id });
          setTimeout(() => {
              startPlayerTurn();
          }, 500);
      }, 6000);
  }

  const playerSelections = initPlayerSelections(gameState.currentPlayer.id);
  gameState.status = 'playing';
  gameState.turnStartTime = Date.now();

  // Actualizar en Firebase si está disponible
  if (db) {
      queueGameStateChange('gameState/currentPlayerIndex', gameState.currentPlayerIndex);
      queueGameStateChange('gameState/status', 'playing');
      queueGameStateChange('gameState/turnStartTime', gameState.turnStartTime);
  }

  io.emit('gameState', {
      board: gameState.board.map(tile => ({
          ...tile,
          value: tile.revealed ? tile.value : null
      })),
      currentPlayer: gameState.currentPlayer,
      players: gameState.players.map(player => ({
          id: player.id,
          username: player.username,
          isBlocked: getUserById(player.id).isBlocked,
          isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
          isConnected: player.isConnected // Asegurarse de enviar el estado de conexión correcto
      })),
      status: 'playing',
      turnStartTime: gameState.turnStartTime,
      totalSelections: playerSelections.totalSelected
  });

  // Guardar estado después de cambiar de turno
  saveGameState();

  console.log(`Fin de startPlayerTurn: estado=${gameState.status}, jugador actual=${gameState.currentPlayer?.username}`);
}

// Configuración de Socket.io
io.on('connection', (socket) => {
  console.log(`Usuario conectado: ${socket.id}`);

  // Evento de prueba para verificar conexión
  socket.on('test', (data) => {
      console.log(`Prueba recibida del cliente ${socket.id}:`, data);
      // Enviar respuesta al cliente
      socket.emit('testResponse', { message: 'Prueba exitosa' });
  });

  // Reconexión de usuario
  socket.on('reconnectUser', ({ userId, username }) => {
      connectedSockets[socket.id] = userId;
      console.log(`Usuario ${username} reconectado con socket ${socket.id}`);

      // Actualizar el socket ID en la lista de jugadores
      const playerIndex = gameState.players.findIndex(player => player.id === userId);
      if (playerIndex !== -1) {
          gameState.players[playerIndex].socketId = socket.id;

          // Marcar al jugador como conectado
          const wasConnected = gameState.players[playerIndex].isConnected;
          gameState.players[playerIndex].isConnected = true;

          // Actualizar en Firebase si está disponible
          if (db) {
              queueGameStateChange(`gameState/players/${playerIndex}/isConnected`, true);
              queueGameStateChange(`gameState/players/${playerIndex}/socketId`, socket.id);
          }

          // Notificar a otros jugadores sobre la reconexión
          if (!wasConnected) {
              io.emit('playerConnectionChanged', {
                  playerId: userId,
                  isConnected: true,
                  username
              });
          }

          // Si no hay jugador actual o el jugador actual está desconectado, 
          // considerar iniciar un nuevo turno
          if (!gameState.currentPlayer || !gameState.currentPlayer.isConnected) {
              startPlayerTurn();
          }
      }

      // Guardar estado después de la reconexión
      saveGameState();
  });

  // Sincronización completa del estado del juego
  socket.on('syncGameState', ({ userId }) => {
      const user = getUserById(userId);
      if (!user) return;

      // Inicializar contadores si no existen
      if (playerTableCount[userId] === undefined) {
          playerTableCount[userId] = 0;
      }

      // Inicializar selecciones del jugador si no existen
      initPlayerSelections(userId);

      // Restaurar estado guardado del jugador si existe
      if (playerGameState[userId]) {
          console.log(`Restaurando estado guardado para ${user.username}`);

          // Enviar estado guardado del tablero
          socket.emit('gameState', {
              board: playerGameState[userId].board,
              currentPlayer: gameState.currentPlayer,
              players: gameState.players.map(player => ({
                  id: player.id,
                  username: player.username,
                  isBlocked: getUserById(player.id).isBlocked,
                  isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
                  isConnected: player.isConnected
              })),
              status: 'playing',
              totalSelections: playerGameState[userId].totalSelected || 0
          });
      } else {
          // Si no hay estado guardado, enviar el estado actual
          socket.emit('gameState', {
              board: gameState.board.map(tile => ({
                  ...tile,
                  value: tile.revealed ? tile.value : null
              })),
              currentPlayer: gameState.currentPlayer,
              players: gameState.players.map(player => ({
                  id: player.id,
                  username: player.username,
                  isBlocked: getUserById(player.id).isBlocked,
                  isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
                  isConnected: player.isConnected
              })),
              status: 'playing',
              totalSelections: gameState.playerSelections[userId]?.totalSelected || 0
          });
      }

      // Enviar puntuación actualizada
      socket.emit('forceScoreUpdate', user.score);

      // Enviar información de la mesa actual
      socket.emit('tablesUpdate', {
          tablesPlayed: playerTableCount[userId] || 0,
          currentTable: globalTableNumber, // Enviar número de mesa global
          maxReached: (playerTableCount[userId] || 0) >= MAX_TABLES_PER_DAY,
          lockReason: (playerTableCount[userId] || 0) >= MAX_TABLES_PER_DAY ?
              'Has alcanzado el límite diario de mesas.' : ''
      });

      // NUEVO: Verificar si el jugador debe mostrar el modal permanente
      if (gameState.playerSelections[userId] && gameState.playerSelections[userId].hasCompletedSelections) {
          socket.emit('showPermanentModal', {
              playerId: userId,
              message: 'para jugar en nuestro juego real escríbenos en WhatsApp y te brindaremos toda la información'
          });
      }

      // Verificar si el jugador está bloqueado por tener 23,000 puntos o menos
      if (user.isLockedDueToScore) {
          socket.emit('scoreLimitReached', {
              message: 'Has alcanzado 23,000 puntos o menos. Tu cuenta ha sido bloqueada temporalmente.'
          });
      }

      // Enviar estado actual de bloqueo
      socket.emit('blockStatusChanged', {
          isBlocked: user.isBlocked,
          isLockedDueToScore: user.isLockedDueToScore,
          message: 'Sincronizando estado del juego'
      });
  });

  // Evento para verificar y corregir estado de las mesas
  socket.on('checkTableStatus', async ({ userId }) => {
      if (!userId) return;

      const user = getUserById(userId);
      if (!user) return;

      // Verificar si el usuario está realmente bloqueado por mesas
      const isReallyBlocked = checkTableLimit(userId);

      // Si es mesa 10 y bloqueado, pero debería poder jugar, desbloquearlo
      if (globalTableNumber === 10 && isReallyBlocked && playerTableCount[userId] <= MAX_TABLES_PER_DAY) {
          console.log(`Corrigiendo bloqueo incorrecto en mesa 10 para ${user.username}`);
          playerTableCount[userId] = Math.min(playerTableCount[userId], MAX_TABLES_PER_DAY - 1);

          // Notificar al usuario
          socket.emit('tablesUpdate', {
              tablesPlayed: playerTableCount[userId],
              currentTable: globalTableNumber,
              maxReached: false,
              lockReason: ''
          });

          // Actualizar en Firebase
          if (db) {
              queueGameStateChange(`gameState/userScores/${userId}/tablesPlayed`, playerTableCount[userId]);
          }

          // Guardar estado corregido
          await saveGameState();
      }

      // Siempre sincronizar el estado actual
      syncPlayerState(userId, socket.id);
  });

  // CAMBIO: Evento para reiniciar las selecciones si hay problemas
  socket.on('resetRowSelections', ({ userId }) => {
      if (!userId) return;

      // Verificar que el usuario exista
      const user = getUserById(userId);
      if (!user) return;

      console.log(`Reiniciando selecciones para ${user.username}`);

      // Reiniciar selecciones para este usuario (SOLO si no ha completado)
      if (gameState.playerSelections[userId] && !gameState.playerSelections[userId].hasCompletedSelections) {
          gameState.playerSelections[userId].totalSelected = 0;
          gameState.playerSelections[userId].showPermanentModal = false;

          // Sincronizar con el cliente
          socket.emit('gameState', {
              totalSelections: 0
          });

          // Actualizar en Firebase
          if (db) {
              queueGameStateChange(`gameState/playerSelections/${userId}/totalSelected`, 0);
              queueGameStateChange(`gameState/playerSelections/${userId}/showPermanentModal`, false);
          }
      } else if (gameState.playerSelections[userId] && gameState.playerSelections[userId].hasCompletedSelections) {
          // Si ya completó, mantener el modal
          socket.emit('showPermanentModal', {
              playerId: userId,
              message: 'para jugar en nuestro juego real escríbenos en WhatsApp y te brindaremos toda la información'
          });
      } else {
          initPlayerSelections(userId);
      }
  });

  // Guardar estado del juego al cerrar sesión - mejorado para garantizar persistencia
  socket.on('saveGameState', async ({ userId }) => {
      if (!userId) return;

      const user = getUserById(userId);
      if (!user) return;

      // Guardar estado específico del jugador
      playerGameState[userId] = {
          board: gameState.board.map(tile => ({
              ...tile,
              value: tile.revealed ? tile.value : null
          })),
          score: user.score,
          prevScore: user.prevScore,
          totalSelected: gameState.playerSelections[userId]?.totalSelected || 0,
          hasCompletedSelections: gameState.playerSelections[userId]?.hasCompletedSelections || false, // NUEVO
          tablesPlayed: playerTableCount[userId] || 0,
          timestamp: Date.now()
      };

      // Actualizar en Firebase si está disponible
      if (db) {
          try {
              await db.ref(`gameState/playerGameStates/${userId}`).set(playerGameState[userId]);
              console.log(`Estado específico de ${user.username} guardado en Firebase`);
          } catch (error) {
              console.error(`Error al guardar estado específico de ${user.username} en Firebase:`, error);
          }
      }

      console.log(`Estado de juego guardado para ${user.username}`);

      // Guardar el estado completo
      await saveGameState();
  });

  // Login
  socket.on('login', (credentials, callback) => {
      const user = users.find(
          u => u.username === credentials.username && u.password === credentials.password
      );

      if (!user) {
          callback({ success: false, message: 'Credenciales incorrectas' });
          return;
      }

      // Comprobar si hay una sesión activa para este usuario
      const existingSocketId = Object.entries(connectedSockets)
          .find(([_, userId]) => userId === user.id)?.[0];

      if (existingSocketId && existingSocketId !== socket.id) {
          // Desconectar la sesión anterior
          io.to(existingSocketId).emit('sessionClosed', 'Se ha iniciado sesión en otro dispositivo');
          // No rechazamos la nueva conexión, sino que reemplazamos la anterior
      }

      // Registrar usuario en el socket
      connectedSockets[socket.id] = user.id;
      console.log(`Usuario ${user.username} autenticado con socket ${socket.id}`);

      // Inicializar las selecciones del jugador si es necesario
      initPlayerSelections(user.id);

      // Responder al cliente con el estado de completado
      callback({
          success: true,
          userId: user.id,
          username: user.username,
          score: user.score,
          isAdmin: user.isAdmin,
          isBlocked: user.isBlocked,
          isLockedDueToScore: user.isLockedDueToScore,
          hasCompletedSelections: gameState.playerSelections[user.id]?.hasCompletedSelections || false // NUEVO
      });
  });

  // Unirse al juego
  socket.on('joinGame', () => {
      const userId = connectedSockets[socket.id];
      if (!userId) return;

      const user = getUserById(userId);
      if (!user) return;

      // Inicializar selecciones del jugador si no existen
      initPlayerSelections(userId);

      // Verificar si el jugador ya está en el juego
      const existingPlayerIndex = gameState.players.findIndex(player => player.id === userId);

      if (existingPlayerIndex === -1) {
          // El jugador no está en el juego, añadirlo
          gameState.players.push({
              id: userId,
              username: user.username,
              socketId: socket.id,
              isConnected: true
          });

          // Actualizar en Firebase si está disponible
          if (db) {
              const newPlayerIndex = gameState.players.length - 1;
              queueGameStateChange(`gameState/players/${newPlayerIndex}`, {
                  id: userId,
                  username: user.username,
                  socketId: socket.id,
                  isConnected: true
              });
          }

          console.log(`Usuario ${user.username} añadido al juego con estado: conectado`);

          // Notificar a todos sobre el nuevo jugador
          io.emit('connectionStatusUpdate', {
              players: [{
                  id: userId,
                  isConnected: true,
                  username: user.username
              }]
          });

          // IMPORTANTE: Forzar el estado a playing explícitamente
          gameState.status = 'playing';

          // Si no hay jugador actual, establecer este jugador como el actual
          // (solo si no es admin, no está bloqueado y no ha completado)
          if (!gameState.currentPlayer && !user.isAdmin && !user.isBlocked && !user.isLockedDueToScore &&
              (!gameState.playerSelections[userId] || !gameState.playerSelections[userId].hasCompletedSelections)) {
              gameState.currentPlayer = gameState.players[gameState.players.length - 1];
              gameState.currentPlayerIndex = gameState.players.length - 1;

              if (db) {
                  queueGameStateChange('gameState/currentPlayerIndex', gameState.currentPlayerIndex);
                  queueGameStateChange('gameState/status', 'playing');
              }
          }

          // Iniciar turno (saltará a los jugadores no elegibles)
          startPlayerTurn();

          // Validar explícitamente que el estado sea 'playing' después de startPlayerTurn
          console.log(`Estado del juego después de startPlayerTurn: ${gameState.status}`);
          if (gameState.status !== 'playing') {
              gameState.status = 'playing';

              if (db) {
                  queueGameStateChange('gameState/status', 'playing');
              }
          }

          // IMPORTANTE: Agregar log para depuración
          console.log(`Emitiendo estado: ${gameState.status}, jugador actual: ${gameState.currentPlayer?.username}`);

          // Emitir estado actualizado inmediatamente
          io.emit('gameState', {
              board: gameState.board.map(tile => ({
                  ...tile,
                  value: tile.revealed ? tile.value : null
              })),
              currentPlayer: gameState.currentPlayer,
              players: gameState.players.map(player => ({
                  id: player.id,
                  username: player.username,
                  isBlocked: getUserById(player.id).isBlocked,
                  isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
                  isConnected: player.isConnected
              })),
              status: 'playing',  // Forzar estado 'playing' explícitamente aquí también
              totalSelections: gameState.playerSelections[userId]?.totalSelected || 0
          });

          // Anunciar a todos los demás clientes
          socket.broadcast.emit('gameState', {
              board: gameState.board.map(tile => ({
                  ...tile,
                  value: tile.revealed ? tile.value : null
              })),
              currentPlayer: gameState.currentPlayer,
              players: gameState.players.map(player => ({
                  id: player.id,
                  username: player.username,
                  isBlocked: getUserById(player.id).isBlocked,
                  isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
                  isConnected: player.isConnected
              })),
              status: 'playing',
              totalSelections: gameState.playerSelections[userId]?.totalSelected || 0
          });

          // Verificar si debe mostrar el modal permanente
          if (gameState.playerSelections[userId] && gameState.playerSelections[userId].hasCompletedSelections) {
              socket.emit('showPermanentModal', {
                  playerId: userId,
                  message: 'para jugar en nuestro juego real escríbenos en WhatsApp y te brindaremos toda la información'
              });
          }

          // Guardar estado
          saveGameState();
      } else {
          // El jugador ya está en el juego, actualizar su estado de conexión
          gameState.players[existingPlayerIndex].socketId = socket.id;
          const wasConnected = gameState.players[existingPlayerIndex].isConnected;
          gameState.players[existingPlayerIndex].isConnected = true;

          // Actualizar en Firebase si está disponible
          if (db) {
              queueGameStateChange(`gameState/players/${existingPlayerIndex}/isConnected`, true);
              queueGameStateChange(`gameState/players/${existingPlayerIndex}/socketId`, socket.id);
          }

          console.log(`Usuario ${user.username} reconectado al juego`);

          // Notificar a todos sobre la reconexión, pero solo si cambió de estado
          if (!wasConnected) {
              io.emit('connectionStatusUpdate', {
                  players: [{
                      id: userId,
                      isConnected: true,
                      username: user.username
                  }]
              });

              // Enviar mensaje a todos los jugadores
              io.emit('message', `${user.username} se ha reconectado al juego`);

              // Si no hay jugador actual o el jugador actual está desconectado,
              // reiniciar los turnos
              if (!gameState.currentPlayer || !gameState.currentPlayer.isConnected) {
                  startPlayerTurn();
              }
          }

          // Asegurarse de que el juego esté en estado 'playing' y haya un jugador actual
          if (gameState.status !== 'playing') {
              gameState.status = 'playing';

              if (db) {
                  queueGameStateChange('gameState/status', 'playing');
              }

              startPlayerTurn(); // Reiniciar el turno si el juego estaba en espera
          }

          if (!gameState.currentPlayer && gameState.players.length > 0) {
              startPlayerTurn(); // Asegurar que haya un turno activo
          }

          // Enviar estado actual al jugador reconectado
          socket.emit('gameState', {
              board: gameState.board.map(tile => ({
                  ...tile,
                  value: tile.revealed ? tile.value : null
              })),
              currentPlayer: gameState.currentPlayer,
              players: gameState.players.map(player => ({
                  id: player.id,
                  username: player.username,
                  isBlocked: getUserById(player.id).isBlocked,
                  isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
                  isConnected: player.isConnected
              })),
              status: 'playing',  // Forzar estado 'playing' explícitamente aquí también
              totalSelections: gameState.playerSelections[userId]?.totalSelected || 0
          });

          // Verificar si debe mostrar el modal permanente
          if (gameState.playerSelections[userId] && gameState.playerSelections[userId].hasCompletedSelections) {
              socket.emit('showPermanentModal', {
                  playerId: userId,
                  message: 'para jugar en nuestro juego real escríbenos en WhatsApp y te brindaremos toda la información'
              });
          }

          // Guardar estado después de la reconexión
          saveGameState();
      }
  });

  // Añade este evento junto a los demás socket.on(...) del servidor
  socket.on('unlockAllTables', async (_, callback) => {
      console.log("Solicitado desbloqueo de emergencia para todas las mesas");

      // Reiniciar contadores para todos los jugadores
      Object.keys(playerTableCount).forEach(userId => {
          playerTableCount[userId] = 0;
      });

      // Actualizar en Firebase si está disponible
      if (db) {
          const updates = {};
          Object.keys(playerTableCount).forEach(userId => {
              updates[`gameState/userScores/${userId}/tablesPlayed`] = 0;
          });

          if (Object.keys(updates).length > 0) {
              try {
                  await db.ref().update(updates);
                  console.log('Contadores de mesa reiniciados en Firebase');
              } catch (error) {
                  console.error('Error al reiniciar contadores de mesa en Firebase:', error);
              }
          }
      }

      // Notificar a todos los clientes
      io.emit('tablesUnlocked', { message: 'Se han desbloqueado todas las mesas.' });

      // Enviar actualización de tableros a todos los jugadores
      for (const player of gameState.players) {
          if (player.socketId && player.isConnected) {
              io.to(player.socketId).emit('tablesUpdate', {
                  tablesPlayed: 0,
                  currentTable: globalTableNumber,
                  maxReached: false,
                  lockReason: ''
              });
          }
      }

      console.log('Todas las mesas desbloqueadas correctamente');

      // Guardar estado
      await saveGameState();

      if (callback) {
          callback({ success: true, message: 'Todas las mesas desbloqueadas correctamente' });
      }
  });

  // CAMBIO: Seleccionar una ficha - Actualizada para el nuevo sistema de 5 fichas
   socket.on('selectTile', async ({ tileIndex, currentScore }) => {
       console.log(`Recibido evento selectTile para ficha ${tileIndex} de socket ${socket.id}`);

       socket.emit('tileSelectResponse', { received: true, tileIndex });

       const userId = connectedSockets[socket.id];
       if (!userId) {
           console.log('Usuario no autenticado, evento ignorado');
           return;
       }

       const user = getUserById(userId);
       if (!user) {
           console.log('Usuario no encontrado, evento ignorado');
           return;
       }

       // No permitir que los administradores jueguen
       if (user.isAdmin) {
           console.log(`El administrador ${user.username} intentó jugar, solo puede observar`);
           socket.emit('message', 'Los administradores solo pueden observar el juego');
           return;
       }

       // Permitir que usuarios bloqueados vean el tablero pero no seleccionen fichas
       if (user.isBlocked) {
           console.log(`Usuario ${user.username} bloqueado, no puede seleccionar fichas`);
           socket.emit('message', 'Tu cuenta está bloqueada. Puedes ver el juego pero no jugar.');
           return;
       }

       // Verificar si el usuario está bloqueado por puntaje
       if (user.isLockedDueToScore) {
           console.log(`Usuario ${user.username} bloqueado por puntaje, no puede seleccionar fichas`);
           socket.emit('scoreLimitReached', {
               message: 'Has alcanzado 23,000 puntos o menos. Contacta al administrador para recargar.'
           });
           return;
       }

       // Verificar límite de mesas
       if (checkTableLimit(userId)) {
           console.log(`Usuario ${user.username} ha alcanzado el límite diario de mesas`);
           socket.emit('tableLimitReached', {
               message: 'Has alcanzado el límite diario de mesas.'
           });
           return;
       }

       // Obtener o inicializar selecciones del jugador
       const playerSelections = initPlayerSelections(userId);

       // NUEVO: Verificar si el jugador ya completó sus 5 selecciones
       if (playerSelections.hasCompletedSelections) {
           console.log(`Usuario ${user.username} ya completó sus 5 selecciones`);
           socket.emit('showPermanentModal', {
               playerId: userId,
               message: 'para jugar en nuestro juego real escríbenos en WhatsApp y te brindaremos toda la información'
           });
           return;
       }

       // Permitir seleccionar si es el único jugador o si es su turno
       if (gameState.players.length > 1 && gameState.currentPlayer && gameState.currentPlayer.id !== userId) {
           console.log(`No es el turno de ${user.username}, es el turno de ${gameState.currentPlayer?.username}`);
           return;
       }

       // Verificar si el tiempo se agotó
       const tiempoTranscurrido = Date.now() - gameState.turnStartTime;
       if (tiempoTranscurrido > 6000) {
           console.log(`Tiempo agotado para ${user.username}, han pasado ${tiempoTranscurrido}ms`);
           socket.emit('message', 'Tiempo agotado para este turno');
           return;
       }

       if (tileIndex < 0 || tileIndex >= gameState.board.length) {
           console.log(`Índice de ficha ${tileIndex} fuera de rango`);
           return;
       }

       // VERIFICACIÓN CRÍTICA: Asegurarse de que no se pueda seleccionar la misma ficha dos veces
       if (gameState.board[tileIndex] && gameState.board[tileIndex].revealed) {
           console.log(`IGNORANDO selección repetida para ficha ${tileIndex}`);
           socket.emit('tileSelectError', { message: 'Esta ficha ya fue seleccionada' });

           // NUEVO: Forzar sincronización del tablero para corregir posibles inconsistencias
           socket.emit('forceGameStateRefresh', {
               board: gameState.board,
               currentPlayer: gameState.currentPlayer,
               players: gameState.players.map(player => ({
                   id: player.id,
                   username: player.username,
                   isBlocked: getUserById(player.id).isBlocked || false,
                   isLockedDueToScore: getUserById(player.id).isLockedDueToScore || false,
                   isConnected: player.isConnected || false
               })),
               status: 'playing',
               totalSelections: gameState.playerSelections[userId]?.totalSelected || 0
           });
           return;
       }

       // Asegurarse de que la ficha exista antes de manipularla
       if (!gameState.board[tileIndex]) {
           console.error(`Ficha en índice ${tileIndex} no existe`);
           socket.emit('tileSelectError', { message: 'Ficha no válida' });
           return;
       }

       // Asegurarse de que los valores de punto son precisamente los esperados
       if (gameState.board[tileIndex].value !== 15000 && gameState.board[tileIndex].value !== -16000) {
           console.error(`VALOR DE FICHA INCORRECTO: ${gameState.board[tileIndex].value}`);
           // Corregir el valor
           gameState.board[tileIndex].value = gameState.board[tileIndex].value > 0 ? 15000 : -16000;
       }

       // CAMBIO: Verificar si ya se seleccionaron 5 fichas en total
       if (playerSelections.totalSelected >= MAX_SELECTIONS_PER_PLAYER) {
           console.log(`Jugador ${user.username} ya seleccionó ${MAX_SELECTIONS_PER_PLAYER} fichas`);
           socket.emit('message', `Ya has seleccionado las ${MAX_SELECTIONS_PER_PLAYER} fichas permitidas`);
           return;
       }

       console.log(`Jugador ${user.username} seleccionó ficha ${tileIndex} (${playerSelections.totalSelected + 1}/${MAX_SELECTIONS_PER_PLAYER})`);

       // CAMBIO: Solo incrementar contador total
       playerSelections.totalSelected++;

       // Marcar explícitamente la ficha como revelada ANTES de emitir el evento
       gameState.board[tileIndex].revealed = true;
       gameState.board[tileIndex].selectedBy = user.username;
       gameState.board[tileIndex].selectedAt = Date.now(); // Añadir timestamp

       // Actualizar en Firebase inmediatamente los valores críticos
       if (db) {
           try {
               const criticalUpdates = {
                   [`gameState/board/${tileIndex}/revealed`]: true,
                   [`gameState/board/${tileIndex}/selectedBy`]: user.username,
                   [`gameState/board/${tileIndex}/selectedAt`]: Date.now(),
                   [`gameState/playerSelections/${userId}/totalSelected`]: playerSelections.totalSelected
               };

               await db.ref().update(criticalUpdates);
               console.log('Selección de ficha actualizada en Firebase');
           } catch (error) {
               console.error('Error al actualizar selección de ficha en Firebase:', error);
           }
       }

       // Guardar estado INMEDIATAMENTE después de la selección
       await saveGameState();

       // Acceder al valor real de la ficha en el tablero del servidor
       const tileValue = gameState.board[tileIndex].value;

       // Verificar si hay una discrepancia grande entre el puntaje del cliente y del servidor
       if (currentScore !== undefined && Math.abs(currentScore - user.score) > 16000) {
           console.warn(`ADVERTENCIA: Posible inconsistencia en puntaje del cliente ${currentScore} vs servidor ${user.score}`);
       }

       // Actualizar puntuación con el valor correcto
       const oldScore = user.score;
       user.prevScore = user.score; // Guardar la puntuación anterior
       user.score += tileValue; // Sumar exactamente el valor de la ficha
       const newScore = user.score;

       // Actualizar en Firebase el puntaje inmediatamente
       if (db) {
           try {
               await db.ref(`gameState/userScores/${userId}`).update({
                   score: newScore,
                   prevScore: oldScore
               });
               console.log('Puntaje actualizado en Firebase');
           } catch (error) {
               console.error('Error al actualizar puntaje en Firebase:', error);
           }
       }

       console.log(`PUNTUACIÓN ACTUALIZADA: ${user.username} ${oldScore} -> ${newScore} (${tileValue})`);

       // IMPORTANTE: Verificar bloqueo por límite de puntos inmediatamente
       if (newScore <= 23000 && !user.isAdmin) {
           checkScoreLimit(user);
       }

       // Añadir información de tipo de sonido correcta
       const soundType = tileValue > 0 ? 'win' : 'lose';

       // CORRECCIÓN CRÍTICA: Emitir eventos con el tablero actualizado a TODOS los jugadores
       io.emit('tileSelected', {
           tileIndex,
           tileValue, // Este valor debe ser correcto desde el servidor
           playerId: userId,
           playerUsername: user.username,
           newScore: newScore,
           totalSelections: playerSelections.totalSelected,
           soundType: soundType,
           timestamp: Date.now(),
           isRevealed: true, // Confirmar explícitamente que está revelada
           // NUEVO: Enviar el tablero completo actualizado
           updatedBoard: gameState.board.map(tile => ({
               revealed: tile.revealed,
               selectedBy: tile.selectedBy,
               value: tile.revealed ? tile.value : null
           }))
       });

       // Enviar actualización de puntaje solo al jugador que seleccionó
       socket.emit('forceScoreUpdate', newScore);

       // CAMBIO: Verificar si el jugador completó las 5 selecciones
       if (playerSelections.totalSelected >= MAX_SELECTIONS_PER_PLAYER) {
           console.log(`${user.username} ha seleccionado ${MAX_SELECTIONS_PER_PLAYER} fichas, mostrando modal permanente`);
           
           // Marcar que debe mostrar el modal permanente PERMANENTEMENTE
           playerSelections.showPermanentModal = true;
           playerSelections.hasCompletedSelections = true; // NUEVO: marcar como completado PERMANENTEMENTE
           
           // Emitir evento para mostrar modal permanente SOLO al jugador que completó
           socket.emit('showPermanentModal', {
               playerId: userId,
               message: 'para jugar en nuestro juego real escríbenos en WhatsApp y te brindaremos toda la información'
           });

           // Actualizar en Firebase
           if (db) {
               queueGameStateChange(`gameState/playerSelections/${userId}/showPermanentModal`, true);
               queueGameStateChange(`gameState/playerSelections/${userId}/hasCompletedSelections`, true);
           }

           // Guardar estado después de completar las 5 fichas
           await saveGameState();

           console.log(`${user.username} COMPLETÓ sus 5 fichas y fue bloqueado permanentemente`);
           return;
       }

       // Verificar si se revelaron todas las fichas del tablero (caso especial)
       if (checkGameOver()) {
           console.log("Todas las fichas han sido reveladas. Reiniciando tablero pero manteniendo puntuaciones");

           // Incrementar contador de mesas para todos los jugadores activos que NO han completado
           for (const player of gameState.players) {
               if (player.isConnected &&
                   !getUserById(player.id)?.isBlocked &&
                   !getUserById(player.id)?.isLockedDueToScore &&
                   !getUserById(player.id)?.isAdmin &&
                   (!gameState.playerSelections[player.id] || !gameState.playerSelections[player.id].hasCompletedSelections)) {
                   incrementTableCount(player.id);
               }
           }

           // Reiniciar solo el tablero
           await resetBoardOnly();

           return;
       }

       // Para múltiples jugadores, pasar al siguiente turno si no completó las 5 fichas
       if (gameState.players.length > 1 && playerSelections.totalSelected < MAX_SELECTIONS_PER_PLAYER) {
           console.log(`${user.username} ha seleccionado ${playerSelections.totalSelected}/${MAX_SELECTIONS_PER_PLAYER} fichas, pasando al siguiente jugador`);
           
           // Pasar al siguiente jugador
           clearTimeout(turnTimer);
           setTimeout(() => {
               startPlayerTurn();
           }, 500);
       }
   });

  // Agregar este nuevo manejador para sincronización forzada
  socket.on('syncScore', async ({ userId }) => {
      console.log(`Solicitada sincronización de puntaje para: ${userId}`);
      const user = getUserById(userId);
      if (user) {
          console.log(`Enviando puntaje actualizado: ${user.score}`);
          socket.emit('directScoreUpdate', user.score);

          // Sincronizar estado completo del juego
          syncPlayerState(userId, socket.id);
      }
  });

  // Evento para recargar puntos (solo para administradores)
  socket.on('rechargePoints', async ({ userId }, callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const targetUser = getUserById(userId);
      if (!targetUser) {
          callback({ success: false, message: 'Usuario no encontrado' });
          return;
      }

      // Incrementar puntuación en 6,000
      targetUser.score += 6000;
      targetUser.prevScore = targetUser.score;

      // Desbloquear al usuario
      targetUser.isBlocked = false;

      // Actualizar en Firebase
      if (db) {
          try {
              await db.ref(`gameState/userScores/${userId}`).update({
                  score: targetUser.score,
                  prevScore: targetUser.prevScore,
                  isBlocked: false
              });
              console.log(`Recarga de puntos para ${targetUser.username} actualizada en Firebase`);
          } catch (error) {
              console.error(`Error al actualizar recarga de puntos en Firebase:`, error);
          }
      }

      // Notificar al usuario si está conectado
      const playerSocketId = gameState.players.find(p => p.id === userId)?.socketId;
      if (playerSocketId) {
          io.to(playerSocketId).emit('forceScoreUpdate', targetUser.score);
          io.to(playerSocketId).emit('message', 'Un administrador ha recargado 6,000 puntos a tu cuenta');
          io.to(playerSocketId).emit('blockStatusChanged', {
              isBlocked: false,
              message: 'Un administrador ha recargado puntos a tu cuenta.'
          });
      }

      // Guardar estado
      await saveGameState();

      callback({ success: true });
  });

  // Evento para reiniciar los contadores de mesas (solo para admin)
  socket.on('adminResetTables', async (callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      await adminResetTableCounters();
      callback({ success: true, message: 'Contadores de mesas reiniciados correctamente' });
  });

  // Obtener lista de jugadores (solo para admins)
  socket.on('getPlayers', (callback) => {
      const userId = connectedSockets[socket.id];
      if (!userId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const user = getUserById(userId);
      if (!user || !user.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      callback({
          success: true,
          players: users.filter(u => !u.isAdmin).map(u => ({
              id: u.id,
              username: u.username,
              score: u.score,
              isBlocked: u.isBlocked,
              isLockedDueToScore: u.isLockedDueToScore
          }))
      });
  });

  // Actualizar puntos (solo para admins)
  socket.on('updatePoints', async ({ userId, points }, callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const targetUser = getUserById(userId);
      if (!targetUser) {
          callback({ success: false, message: 'Usuario no encontrado' });
          return;
      }

      // Actualizar puntuación
      const newScore = updateUserScore(userId, points);

      // Actualizar en Firebase
      if (db) {
          try {
              await db.ref(`gameState/userScores/${userId}`).update({
                  score: newScore,
                  prevScore: targetUser.prevScore
              });
              console.log(`Actualización de puntos para ${targetUser.username} registrada en Firebase`);
          } catch (error) {
              console.error('Error al actualizar puntos en Firebase:', error);
          }
      }

      // Notificar al usuario, si está conectado
      const playerSocketId = gameState.players.find(p => p.id === userId)?.socketId;
      if (playerSocketId) {
          io.to(playerSocketId).emit('scoreUpdate', newScore);
      }

      // Actualizar lista de jugadores para todos los admins
      io.emit('playersUpdate', users.filter(u => !u.isAdmin).map(u => ({
          id: u.id,
          username: u.username,
          score: u.score,
          isBlocked: u.isBlocked,
          isLockedDueToScore: u.isLockedDueToScore
      })));

      // Guardar estado después de actualizar puntos
      await saveGameState();

      callback({ success: true });
  });

  // Bloquear/desbloquear usuario (solo para admins)
  socket.on('toggleBlockUser', async ({ userId }, callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const targetUser = getUserById(userId);
      if (!targetUser) {
          callback({ success: false, message: 'Usuario no encontrado' });
          return;
      }

      // Cambiar estado de bloqueo
      targetUser.isBlocked = !targetUser.isBlocked;

      // Actualizar en Firebase
      if (db) {
          try {
              await db.ref(`gameState/userScores/${userId}/isBlocked`).set(targetUser.isBlocked);
              console.log(`Estado de bloqueo para ${targetUser.username} actualizado en Firebase`);
          } catch (error) {
              console.error('Error al actualizar estado de bloqueo en Firebase:', error);
          }
      }

      // Notificar al usuario, si está conectado
      const playerSocketId = gameState.players.find(p => p.id === userId)?.socketId;
      if (playerSocketId) {
          if (targetUser.isBlocked) {
              io.to(playerSocketId).emit('blockStatusChanged', {
                  isBlocked: true,
                  message: 'Tu cuenta ha sido bloqueada por el administrador. Puedes seguir viendo el juego pero no jugar.'
              });
              io.to(playerSocketId).emit('message', 'Tu cuenta ha sido bloqueada por el administrador. Puedes seguir viendo el juego pero no jugar.');
          } else {
              io.to(playerSocketId).emit('blockStatusChanged', {
                  isBlocked: false,
                  message: 'Tu cuenta ha sido desbloqueada por el administrador.'
              });
              io.to(playerSocketId).emit('message', 'Tu cuenta ha sido desbloqueada por el administrador.');
          }
      }

      // Si el jugador bloqueado era el jugador actual, pasar al siguiente
      if (targetUser.isBlocked && gameState.currentPlayer && gameState.currentPlayer.id === userId) {
          clearTimeout(turnTimer);
          setTimeout(() => {
              startPlayerTurn();
          }, 500);
      }

      // Actualizar lista de jugadores para todos los admins
      io.emit('playersUpdate', users.filter(u => !u.isAdmin).map(u => ({
          id: u.id,
          username: u.username,
          score: u.score,
          isBlocked: u.isBlocked,
          isLockedDueToScore: u.isLockedDueToScore
      })));

      // Actualizar el estado del juego para todos
      io.emit('gameState', {
          board: gameState.board.map(tile => ({
              ...tile,
              value: tile.revealed ? tile.value : null
          })),
          currentPlayer: gameState.currentPlayer,
          players: gameState.players.map(player => ({
              id: player.id,
              username: player.username,
              isBlocked: getUserById(player.id).isBlocked,
              isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
              isConnected: player.isConnected
          })),
          status: 'playing',
          turnStartTime: gameState.turnStartTime
      });

      // Guardar estado después de cambiar bloqueo
      await saveGameState();

      callback({ success: true });
  });

  // Evento para desbloquear usuario por puntaje (solo para admins)
  socket.on('unlockUserScore', async ({ userId }, callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const targetUser = getUserById(userId);
      if (!targetUser) {
          callback({ success: false, message: 'Usuario no encontrado' });
          return;
      }

      // Desbloquear al usuario
      targetUser.isLockedDueToScore = false;

      // Actualizar en Firebase
      if (db) {
          try {
              await db.ref(`gameState/userScores/${userId}/isLockedDueToScore`).set(false);
              console.log(`Desbloqueo por puntaje para ${targetUser.username} actualizado en Firebase`);
          } catch (error) {
              console.error('Error al actualizar desbloqueo por puntaje en Firebase:', error);
          }
      }

      // Notificar al usuario en tiempo real
      const playerSocketId = gameState.players.find(p => p.id === userId)?.socketId;
      if (playerSocketId) {
          io.to(playerSocketId).emit('blockStatusChanged', {
              isLockedDueToScore: false,
              message: 'Un administrador ha desbloqueado tu cuenta por puntaje.'
          });
          io.to(playerSocketId).emit('userUnlocked', {
              message: 'Un administrador ha desbloqueado tu cuenta por puntaje.'
          });
      }

      // Guardar estado
      await saveGameState();

      callback({ success: true });
  });

  // Evento para cambiar el nombre de usuario (solo para admins) - MEJORADO
  socket.on('changeUsername', async ({ userId, newUsername }, callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const targetUser = getUserById(userId);
      if (!targetUser) {
          callback({ success: false, message: 'Usuario no encontrado' });
          return;
      }

      // Verificar que el nuevo nombre no esté en uso
      const existingUser = users.find(u => u.username.toLowerCase() === newUsername.toLowerCase() && u.id !== userId);
      if (existingUser) {
          callback({ success: false, message: 'Este nombre de usuario ya está en uso' });
          return;
      }

      // Cambiar el nombre de usuario
      const oldUsername = targetUser.username;
      targetUser.username = newUsername;

      // Actualizar en la lista de jugadores activos
      const playerIndex = gameState.players.findIndex(p => p.id === userId);
      if (playerIndex !== -1) {
          gameState.players[playerIndex].username = newUsername;
      }

      // Actualizar en Firebase si está disponible
      if (db) {
          try {
              // Guardar tanto en userScores como en modifiedUsers
              await db.ref(`gameState/userScores/${userId}/username`).set(newUsername);

              // Actualizar la lista de usuarios modificados
              const modifiedUsersSnapshot = await db.ref('gameState/modifiedUsers').once('value');
              let modifiedUsers = modifiedUsersSnapshot.val() || [];

              // Convertir a array si no lo es
              if (!Array.isArray(modifiedUsers)) {
                  modifiedUsers = Object.values(modifiedUsers);
              }

              const userIndex = modifiedUsers.findIndex(u => u.id === userId);

              if (userIndex !== -1) {
                  modifiedUsers[userIndex].username = newUsername;
              } else {
                  modifiedUsers.push({
                      id: userId,
                      username: newUsername,
                      password: targetUser.password,
                      score: targetUser.score,
                      prevScore: targetUser.prevScore,
                      isAdmin: targetUser.isAdmin,
                      isBlocked: targetUser.isBlocked,
                      isLockedDueToScore: targetUser.isLockedDueToScore
                  });
              }

              await db.ref('gameState/modifiedUsers').set(modifiedUsers);

              if (playerIndex !== -1) {
                  await db.ref(`gameState/players/${playerIndex}/username`).set(newUsername);
              }
              console.log(`Nombre de usuario cambiado de ${oldUsername} a ${newUsername} y guardado en Firebase`);
          } catch (error) {
              console.error('Error al actualizar nombre de usuario en Firebase:', error);
          }
      }

      // IMPORTANTE: Guardar estado completo inmediatamente
      await saveGameState();

      // Notificar al usuario si está conectado
      const playerSocketId = gameState.players.find(p => p.id === userId)?.socketId;
      if (playerSocketId) {
          io.to(playerSocketId).emit('usernameChanged', {
              newUsername: newUsername,
              message: `Tu nombre de usuario ha sido cambiado a: ${newUsername}`
          });
      }

      // Actualizar lista de jugadores para todos
      io.emit('playersUpdate', users.filter(u => !u.isAdmin).map(u => ({
          id: u.id,
          username: u.username,
          score: u.score,
          isBlocked: u.isBlocked,
          isLockedDueToScore: u.isLockedDueToScore
      })));

      console.log(`Cambio de nombre completado y persistido: ${oldUsername} -> ${newUsername}`);
      callback({ success: true, message: `Nombre cambiado de ${oldUsername} a ${newUsername}` });
  });

  // Evento para cambiar la contraseña (solo para admins) - MEJORADO
  socket.on('changePassword', async ({ userId, newPassword }, callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const targetUser = getUserById(userId);
      if (!targetUser) {
          callback({ success: false, message: 'Usuario no encontrado' });
          return;
      }

      // Validar longitud mínima de contraseña
      if (newPassword.length < 6) {
          callback({ success: false, message: 'La contraseña debe tener al menos 6 caracteres' });
          return;
      }

      // Cambiar la contraseña
      targetUser.password = newPassword;

      // Actualizar en Firebase si está disponible
      if (db) {
          try {
              // Guardar tanto en userScores como en modifiedUsers
              await db.ref(`gameState/userScores/${userId}/password`).set(newPassword);
              await db.ref(`gameState/userScores/${userId}/passwordChanged`).set(true);
              await db.ref(`gameState/userScores/${userId}/passwordChangedAt`).set(Date.now());

              // Actualizar la lista de usuarios modificados
              const modifiedUsersSnapshot = await db.ref('gameState/modifiedUsers').once('value');
              let modifiedUsers = modifiedUsersSnapshot.val() || [];

              // Convertir a array si no lo es
              if (!Array.isArray(modifiedUsers)) {
                  modifiedUsers = Object.values(modifiedUsers);
              }

              const userIndex = modifiedUsers.findIndex(u => u.id === userId);

              if (userIndex !== -1) {
                  modifiedUsers[userIndex].password = newPassword;
              } else {
                  modifiedUsers.push({
                      id: userId,
                      username: targetUser.username,
                      password: newPassword,
                      score: targetUser.score,
                      prevScore: targetUser.prevScore,
                      isAdmin: targetUser.isAdmin,
                      isBlocked: targetUser.isBlocked,
                      isLockedDueToScore: targetUser.isLockedDueToScore
                  });
              }

              await db.ref('gameState/modifiedUsers').set(modifiedUsers);

              console.log(`Contraseña actualizada para ${targetUser.username} y guardada en Firebase`);
          } catch (error) {
              console.error('Error al registrar cambio de contraseña en Firebase:', error);
          }
      }

      // IMPORTANTE: Guardar estado completo inmediatamente
      await saveGameState();

      // Notificar al usuario si está conectado
      const playerSocketId = gameState.players.find(p => p.id === userId)?.socketId;
      if (playerSocketId) {
          io.to(playerSocketId).emit('passwordChanged', {
              message: 'Tu contraseña ha sido actualizada por el administrador. Deberás usar la nueva contraseña en tu próximo inicio de sesión.'
          });
      }

      console.log(`Cambio de contraseña completado y persistido para: ${targetUser.username}`);
      callback({ success: true, message: `Contraseña actualizada para ${targetUser.username}` });
  });

  // Reiniciar juego (solo para admins)
  socket.on('resetGame', async (callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      await resetGame();
      callback({ success: true });
  });

  // Actualización directa de puntos (para admin) - NUEVO
  socket.on('directSetPoints', async ({ userId, newPoints }, callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const targetUser = getUserById(userId);
      if (!targetUser) {
          callback({ success: false, message: 'Usuario no encontrado' });
          return;
      }

      // Establecer puntuación directamente
      targetUser.prevScore = targetUser.score;
      targetUser.score = parseInt(newPoints, 10);

      // Actualizar en Firebase
      if (db) {
          try {
              await db.ref(`gameState/userScores/${userId}`).update({
                  score: targetUser.score,
                  prevScore: targetUser.prevScore
              });
              console.log(`Puntuación fijada directamente para ${targetUser.username} en Firebase`);
          } catch (error) {
              console.error('Error al fijar puntuación directamente en Firebase:', error);
          }
      }

      // Verificar si debe ser bloqueado
      checkScoreLimit(targetUser);

      // Guardar estado después de actualizar puntos
      await saveGameState();

      // Notificar al usuario, si está conectado
      const playerSocketId = gameState.players.find(p => p.id === userId)?.socketId;
      if (playerSocketId) {
          io.to(playerSocketId).emit('forceScoreUpdate', targetUser.score);

          // Notificar si quedó bloqueado por puntaje
          if (targetUser.isLockedDueToScore) {
              io.to(playerSocketId).emit('blockStatusChanged', {
                  isLockedDueToScore: true,
                  message: 'Has alcanzado 23,000 puntos o menos y has sido bloqueado temporalmente.'
              });
          }
      }

      // Actualizar lista de jugadores para todos los admins
      io.emit('playersUpdate', users.filter(u => !u.isAdmin).map(u => ({
          id: u.id,
          username: u.username,
          score: u.score,
          isBlocked: u.isBlocked,
          isLockedDueToScore: u.isLockedDueToScore
      })));

      callback({ success: true });
  });

  // Evento para desbloquear mesas (solo para admins)
  socket.on('unlockTables', async ({ userId }, callback) => {
      const adminId = connectedSockets[socket.id];
      if (!adminId) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const admin = getUserById(adminId);
      if (!admin || !admin.isAdmin) {
          callback({ success: false, message: 'No autorizado' });
          return;
      }

      const targetUser = getUserById(userId);
      if (!targetUser) {
          callback({ success: false, message: 'Usuario no encontrado' });
          return;
      }

      // Reiniciar contador de mesas para este usuario
      playerTableCount[userId] = 0;

      // Actualizar en Firebase
      if (db) {
          try {
              await db.ref(`gameState/userScores/${userId}/tablesPlayed`).set(0);
              console.log(`Contador de mesas para ${targetUser.username} reiniciado en Firebase`);
          } catch (error) {
              console.error('Error al reiniciar contador de mesas en Firebase:', error);
          }
      }

      // Notificar al usuario
      const playerSocketId = gameState.players.find(p => p.id === userId)?.socketId;
      if (playerSocketId) {
          io.to(playerSocketId).emit('tablesUnlocked');
          io.to(playerSocketId).emit('tablesUpdate', {
              tablesPlayed: 0,
              currentTable: globalTableNumber,
              maxReached: false,
              lockReason: ''
          });
      }

      // Guardar estado
      await saveGameState();

      callback({ success: true });
  });

  // Salir del juego
  socket.on('leaveGame', () => {
      const userId = connectedSockets[socket.id];
      if (!userId) return;

      // Marcar al jugador como desconectado
      const playerIndex = gameState.players.findIndex(player => player.id === userId);
      if (playerIndex !== -1) {
          const player = gameState.players[playerIndex];
          player.isConnected = false;

          // Actualizar en Firebase
          if (db) {
              queueGameStateChange(`gameState/players/${playerIndex}/isConnected`, false);
          }

          // Notificar a todos los clientes sobre la desconexión
          io.emit('playerConnectionChanged', {
              playerId: userId,
              isConnected: false,
              username: player.username
          });

          console.log(`Jugador ${player.username} marcado como desconectado al abandonar el juego`);

          // Si era el turno de este jugador, pasar al siguiente inmediatamente
          if (gameState.currentPlayer && gameState.currentPlayer.id === userId) {
              clearTimeout(turnTimer);

              // Pasar al siguiente jugador después de un momento
              setTimeout(() => {
                  startPlayerTurn();
              }, 1000);
          }

          // Guardar estado específico del jugador
          playerGameState[userId] = {
              board: gameState.board.map(tile => ({
                  ...tile,
                  value: tile.revealed ? tile.value : null
              })),
              score: getUserById(userId).score,
              prevScore: getUserById(userId).prevScore,
              totalSelected: gameState.playerSelections[userId]?.totalSelected || 0,
              hasCompletedSelections: gameState.playerSelections[userId]?.hasCompletedSelections || false, // NUEVO
              tablesPlayed: playerTableCount[userId] || 0,
              timestamp: Date.now()
          };

          // Actualizar en Firebase
          if (db) {
              queueGameStateChange(`gameState/playerGameStates/${userId}`, playerGameState[userId]);
          }

          // Actualizar estado para todos
          io.emit('gameState', {
              board: gameState.board.map(tile => ({
                  ...tile,
                  value: tile.revealed ? tile.value : null
              })),
              currentPlayer: gameState.currentPlayer,
              players: gameState.players.map(player => ({
                  id: player.id,
                  username: player.username,
                  isBlocked: getUserById(player.id).isBlocked,
                  isLockedDueToScore: getUserById(player.id).isLockedDueToScore,
                  isConnected: player.isConnected
              })),
              status: 'playing', // Mantener el estado como 'playing' siempre
              turnStartTime: gameState.turnStartTime
          });

          // Guardar estado después de salir del juego
          saveGameState();
      }
  });

  // Desconexión
  socket.on('disconnect', () => {
      console.log(`Usuario desconectado: ${socket.id}`);

      const userId = connectedSockets[socket.id];
      if (userId) {
          delete connectedSockets[socket.id];

          // Guardar estado específico del jugador antes de marcar como desconectado
          if (userId) {
              const user = getUserById(userId);
              if (user) {
                  playerGameState[userId] = {
                      board: gameState.board.map(tile => ({
                          ...tile,
                          value: tile.revealed ? tile.value : null
                      })),
                      score: user.score,
                      prevScore: user.prevScore,
                      totalSelected: gameState.playerSelections[userId]?.totalSelected || 0,
                      hasCompletedSelections: gameState.playerSelections[userId]?.hasCompletedSelections || false, // NUEVO
                      tablesPlayed: playerTableCount[userId] || 0,
                      timestamp: Date.now()
                  };

                  // Actualizar en Firebase
                  if (db) {
                      queueGameStateChange(`gameState/playerGameStates/${userId}`, playerGameState[userId]);
                  }
              }
          }

          // Marcar al jugador como desconectado pero mantenerlo en la lista
          const playerIndex = gameState.players.findIndex(player => player.id === userId);
          if (playerIndex !== -1) {
              gameState.players[playerIndex].isConnected = false;

              // Actualizar en Firebase
              if (db) {
                  queueGameStateChange(`gameState/players/${playerIndex}/isConnected`, false);
              }

              console.log(`Jugador ${gameState.players[playerIndex].username} marcado como desconectado`);

              // Si era el turno de este jugador, pasar al siguiente inmediatamente
              if (gameState.currentPlayer && gameState.currentPlayer.id === userId) {
                  clearTimeout(turnTimer);
                  console.log(`Saltando el turno del jugador desconectado ${gameState.players[playerIndex].username}`);
                  setTimeout(() => {
                      startPlayerTurn();
                  }, 1000);
              }

              // Notificar a todos los clientes sobre la desconexión
              io.emit('playerConnectionChanged', {
                  playerId: userId,
                  isConnected: false,
                  username: gameState.players[playerIndex].username
              });
          }

          // Guardar estado después de la desconexión
          saveGameState();
      }
  });

  socket.on('completeBoard', async ({ userId }) => {
      const user = getUserById(userId);
      if (!user) return;

      console.log(`Usuario ${user.username} completó su tablero, pero esperando a que se revelen todas las fichas`);

      // Ya no invocamos resetBoardOnly() ni incrementamos el contador
      // Solo enviamos un mensaje al usuario
      socket.emit('message', 'Has completado tus selecciones. Esperando a que se revelen todas las fichas del tablero.');

      // Verificar estados de bloqueo para detectar inconsistencias
      verifyBlockingStates();

      // Guardar estado
      await saveGameState();
  });

  // Ping/pong para detectar desconexiones
  socket.on('ping', (data, callback) => {
      if (typeof callback === 'function') {
          callback({
              status: 'active',
              timestamp: Date.now()
          });
      }
  });
});

// Configurar guardado periódico más frecuente
setInterval(async () => {
  try {
      await saveGameState();
  } catch (error) {
      console.error('Error en guardado periódico:', error);
  }
}, 30 * 1000); // Cada 30 segundos

// Función para verificar estado de Firebase y reconectar si es necesario
const checkFirebaseConnection = async () => {
  if (!db) return; // Si Firebase no está configurado, no hacer nada

  try {
      // Intento de escritura para verificar conexión
      const testRef = db.ref('connection_test');
      await testRef.set({
          timestamp: Date.now(),
          serverTime: admin.database.ServerValue.TIMESTAMP
      });
      console.log('Conexión a Firebase verificada correctamente');
  } catch (error) {
      console.error('Error en la conexión a Firebase, intentando reconectar:', error);

      // Intentar reconectar
      try {
          // En un entorno real, aquí iría código para reinicializar la conexión
          // Pero en Firebase Admin SDK no es necesario ya que maneja reconexiones automáticamente
          console.log('Firebase maneja reconexiones automáticamente');

          // Forzar sincronización después de reconectar
          await saveGameState();
          console.log('Estado sincronizado después de verificar conexión');
      } catch (reconnectError) {
          console.error('Error al reconectar con Firebase:', reconnectError);
      }
  }
};

// Verificar la conexión a Firebase cada 5 minutos
setInterval(checkFirebaseConnection, 5 * 60 * 1000);

// Limpieza de memoria en desuso - cada 30 minutos
setInterval(() => {
  // Limpiar datos de conexiones inactivas
  let disconnectedCount = 0;

  // Verificar y limpiar conexiones inactivas
  for (const socketId in connectedSockets) {
      const userId = connectedSockets[socketId];
      const playerIndex = gameState.players.findIndex(player => player.id === userId);

      if (playerIndex !== -1 && !gameState.players[playerIndex].isConnected) {
          // Si el jugador lleva más de 1 hora desconectado, eliminarlo de la lista
          const lastActivity = playerGameState[userId]?.timestamp || 0;
          if (Date.now() - lastActivity > 60 * 60 * 1000) {
              delete connectedSockets[socketId];
              disconnectedCount++;
          }
      }
  }

  if (disconnectedCount > 0) {
      console.log(`Limpieza periódica: ${disconnectedCount} conexiones inactivas eliminadas`);
  }

  // Siempre guardar después de la limpieza
  saveGameState();
}, 30 * 60 * 1000);

// Endpoint para verificar la configuración de CORS (para depuración)
app.get('/cors-config', (req, res) => {
  res.json({
      corsOrigins: Array.isArray(corsOptions.origin) ? corsOptions.origin : [corsOptions.origin],
      environment: process.env.NODE_ENV,
      clientUrl: process.env.CLIENT_URL
  });
});

// Endpoint para verificar estado de Firebase
app.get('/firebase-status', async (req, res) => {
  if (!db) {
      return res.json({
          status: 'not_configured',
          message: 'Firebase no está configurado en este servidor'
      });
  }

  try {
      // Intento de escritura para verificar conexión
      const testRef = db.ref('health_check');
      const result = await testRef.set({
          timestamp: Date.now(),
          serverTime: admin.database.ServerValue.TIMESTAMP
      });

      res.json({
          status: 'connected',
          message: 'Conexión a Firebase funcionando correctamente',
          timestamp: Date.now()
      });
  } catch (error) {
      res.status(500).json({
          status: 'error',
          message: 'Error en la conexión a Firebase',
          error: error.message
      });
  }
});

// Endpoint para verificar el estado del juego actual
app.get('/game-state-summary', (req, res) => {
  // Proporcionar un resumen del estado actual sin datos sensibles
  const summary = {
      totalPlayers: gameState.players.length,
      connectedPlayers: gameState.players.filter(p => p.isConnected).length,
      revealedTiles: gameState.board.filter(t => t.revealed).length,
      tableNumber: globalTableNumber,
      lastSaved: new Date().toISOString(),
      usernames: users.map(u => ({ id: u.id, username: u.username, isAdmin: u.isAdmin }))
  };

  res.json(summary);
});

// Logs de inicio
console.log('Entorno:', process.env.NODE_ENV);
console.log('URL del cliente:', process.env.CLIENT_URL || 'https://juegoclientedemo.onrender.com');
console.log('Firebase:', db ? 'Configurado' : 'No configurado');
console.log('Base de datos Firebase:', 'https://juegomemoriademo-default-rtdb.firebaseio.com/');

// Iniciar servidor
const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Servidor ejecutándose en el puerto ${PORT}`);
  console.log('=================================');
  console.log('SERVIDOR 3 - JUEGO MEMORIA CUARTO');
  console.log('=================================');
  console.log('Usuarios disponibles:');
  console.log('- Condor (vuela741)');
  console.log('- Colibrí (rapido852)');
  console.log('- Tucan (colores963)');
  console.log('- Quetzal (sagrado159)');
  console.log('- Flamingo (rosado753)');
  console.log('- Gaviota (marina426)');
  console.log('- Pelicano (pescador817)');
  console.log('- Canario (amarillo294)');
  console.log('- Cisne (elegante685)');
  console.log('- Gorrion (urbano372)');
  console.log('- Admin (admin1998)');
  console.log('=================================');

  // Verificar Firebase al inicio
  if (db) {
      checkFirebaseConnection()
          .then(() => console.log('Verificación inicial de Firebase completada'))
          .catch(err => console.error('Error en verificación inicial de Firebase:', err));
  }
});

// Ruta básica para comprobar que el servidor está funcionando
app.get('/', (req, res) => {
  res.send('Servidor del juego de memoria (Servidor 3 - juego-memoria-cuarto) funcionando correctamente');
});

// Manejo de errores no capturados
process.on('uncaughtException', (error) => {
  console.error('Error no capturado:', error);
  // Intentar guardar el estado antes de salir
  saveGameState().then(() => {
      console.log('Estado guardado de emergencia');
  }).catch((err) => {
      console.error('Error al guardar estado de emergencia:', err);
  });
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Promesa rechazada no manejada:', reason);
});

// Manejo graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM recibido, cerrando servidor gracefully...');

  try {
      // Guardar estado final
      await saveGameState();
      console.log('Estado final guardado');

      // Cerrar servidor
      server.close(() => {
          console.log('Servidor cerrado');
          process.exit(0);
      });

      // Forzar cierre después de 10 segundos
      setTimeout(() => {
          console.error('No se pudo cerrar el servidor gracefully, forzando cierre...');
          process.exit(1);
      }, 10000);
  } catch (error) {
      console.error('Error durante el cierre:', error);
      process.exit(1);
  }
});

// Exportar el servidor para testing si es necesario
module.exports = { app, server };