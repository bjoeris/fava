import { $, $$, handleJSON } from './helpers';
import e from './events';
import router from './router';

// let dataTransferFiles = [];

e.on('form-submit-ingest-upload', (form) => {
  const formData = new FormData(form);

  $.fetch(form.getAttribute('data-url'), {
    method: 'PUT',
    body: formData,
  })
  .then(handleJSON)
  .then((data) => {
    e.trigger('info', data.message);
    e.trigger('close-overlay');
    router.navigate(new URL(data.redirect, window.location.href).toString()); 
  }, (error) => {
    e.trigger('error', `Upload error: ${error}`);
  });
});

