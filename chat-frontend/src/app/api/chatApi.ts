let API_BASE_URL = 'http://127.0.0.1:8080';

export interface Room {
  id: string;
  name: string;
  created_by: string;
  created_at: string;
}

export interface Message {
  id: string;
  room_id: string;
  user_id: string;
  content: string;
  message_type: string;
  timestamp: number;
}

export interface MessageCreate {
  id?: string; 
  room_id: string;
  user_id: string;
  content: string;
}

export interface RoomCreate {
  name: string;
}

export const setApiBaseUrl = (url: string) => {
  API_BASE_URL = url;
};

export const getApiBaseUrl = () => {
  return API_BASE_URL;
};


export const fetchRooms = async (): Promise<Room[]> => {
  try {
    const response = await fetch(`${API_BASE_URL}/rooms`);
    
    if (response.status === 307) {
      const redirectUrl = response.headers.get('Location');
      if (redirectUrl) {
        
        const urlObj = new URL(redirectUrl);
        const newBaseUrl = `${urlObj.protocol}//${urlObj.host}`;
        setApiBaseUrl(newBaseUrl);

        return fetchRooms();
      }
    }
    
    if (!response.ok) {
      throw new Error(`Failed to fetch rooms: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Error fetching rooms:', error);
    throw error;
  }
};

export const createRoom = async (room: RoomCreate): Promise<Room> => {
  try {
    const response = await fetch(`${API_BASE_URL}/rooms`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(room),
    });

    if (response.status === 307) {
      const redirectUrl = response.headers.get('Location');
      if (redirectUrl) {
        const urlObj = new URL(redirectUrl);
        const newBaseUrl = `${urlObj.protocol}//${urlObj.host}`;
        setApiBaseUrl(newBaseUrl);

        return createRoom(room);
      }
    }
    
    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(`Failed to create room: ${response.status} - ${errorBody}`);
    }

    const roomResponse = await response.json();
    if (roomResponse.status === "success") {
      return {
        id: roomResponse.room_id,
        name: roomResponse.name,
        created_by: "Unknown",
        created_at: new Date(roomResponse.created_at * 1000).toISOString()
      };
    } else {
      throw new Error(`Failed to create room: ${roomResponse.detail || 'Unknown server error'}`);
    }
  } catch (error) {
    console.error('Error creating room:', error);
    throw error;
  }
};

export const fetchMessages = async (roomId: string): Promise<Message[]> => {
  try {
    const response = await fetch(`${API_BASE_URL}/rooms/${roomId}/messages`);

    if (response.status === 307) {
      const redirectUrl = response.headers.get('Location');
      if (redirectUrl) {
        const urlObj = new URL(redirectUrl);
        const newBaseUrl = `${urlObj.protocol}//${urlObj.host}`;
        setApiBaseUrl(newBaseUrl);

        return fetchMessages(roomId);
      }
    }
    
    if (!response.ok) {
      throw new Error(`Failed to fetch messages: ${response.status}`);
    }

    const responseData = await response.json();
    let rawMessages: any[] = [];

    if (Array.isArray(responseData)) {
      rawMessages = responseData;
    } else if (responseData && Array.isArray(responseData.messages)) {
      rawMessages = responseData.messages;
    } else {
      console.warn('API response did not contain messages in expected format:', responseData);
      return [];
    }

    return rawMessages.map((msg: any): Message => ({
      id: msg.message_id,
      room_id: msg.room_id,
      user_id: msg.user_id,
      content: msg.content,
      message_type: msg.message_type,
      timestamp: msg.created_at,
    }));
  } catch (error) {
    console.error(`Error fetching messages for room ${roomId}:`, error);
    throw error;
  }
};

export const sendMessage = async (message: MessageCreate): Promise<{ message_id: string }> => {
  try {

    const messageData = {
      ...message,
      id: message.id || crypto.randomUUID()
    };

    const response = await fetch(`${API_BASE_URL}/send_message`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(messageData),
    });

    if (response.status === 307) {
      const redirectUrl = response.headers.get('Location');
      if (redirectUrl) {
        const urlObj = new URL(redirectUrl);
        const newBaseUrl = `${urlObj.protocol}//${urlObj.host}`;
        setApiBaseUrl(newBaseUrl);

        return sendMessage(message);
      }
    }
    
    if (!response.ok) {
      throw new Error(`Failed to send message: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Error sending message:', error);
    throw error;
  }
};

export const getRaftState = async () => {
  try {
    const response = await fetch(`${API_BASE_URL}/raft_state`);
    
    if (!response.ok) {
      throw new Error(`Failed to get Raft state: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Error getting Raft state:', error);
    throw error;
  }
};